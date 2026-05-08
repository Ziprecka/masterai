"""Persistent memory store for the overseer.

Three tables backed by SQLite at ``~/.overseer/memory.db``:

  * ``events``        — every BaseEvent that flows over the bus, JSON-blobbed
  * ``notes``         — free-form key/value the conversation agent writes
  * ``task_history``  — completed tasks with their outcomes

Recall uses FTS5 over notes + recent task history. The interface
(``MemoryStore.recall``) is the same shape an embedding-based store would
expose, so the SQLite implementation is swappable later without churning
callers.
"""
from __future__ import annotations

import asyncio
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .bus import bus
from .events import MemoryWritten


@dataclass
class TaskOutcome:
    task_id: str
    project_id: str | None
    title: str
    outcome: str  # "merged" | "failed" | "killed" | "discarded"
    detail: str = ""


class MemoryStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = (path or Path.home() / ".overseer" / "memory.db").expanduser()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # check_same_thread=False so async tasks on different loop threads
        # can share the connection. We serialize via _lock below.
        self._conn = sqlite3.connect(self.path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._lock = asyncio.Lock()
        self._has_fts = self._init_schema()

    def _init_schema(self) -> bool:
        c = self._conn
        c.executescript(
            """
            CREATE TABLE IF NOT EXISTS events (
                id TEXT PRIMARY KEY,
                ts TEXT NOT NULL,
                type TEXT NOT NULL,
                agent_id TEXT,
                project_id TEXT,
                payload TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_events_project ON events(project_id);
            CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);

            CREATE TABLE IF NOT EXISTS notes (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );

            CREATE TABLE IF NOT EXISTS task_history (
                task_id TEXT PRIMARY KEY,
                project_id TEXT,
                title TEXT NOT NULL,
                outcome TEXT NOT NULL,
                detail TEXT,
                completed_at TEXT NOT NULL
            );
            """
        )
        # FTS5 is part of the SQLite amalgamation but not always enabled in
        # custom builds. Detect at runtime so the store still functions
        # without it (recall just falls back to LIKE).
        try:
            c.executescript(
                """
                CREATE VIRTUAL TABLE IF NOT EXISTS notes_fts USING fts5(
                    key, value, content='notes', content_rowid='rowid'
                );
                CREATE VIRTUAL TABLE IF NOT EXISTS history_fts USING fts5(
                    title, detail, content='task_history', content_rowid='rowid'
                );
                """
            )
            c.commit()
            return True
        except sqlite3.OperationalError:
            c.commit()
            return False

    # ────────────────────────── notes ──────────────────────────

    async def write_note(self, key: str, value: str) -> None:
        ts = _now()
        async with self._lock:
            self._conn.execute(
                "INSERT INTO notes(key, value, updated_at) VALUES(?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
                (key, value, ts),
            )
            if self._has_fts:
                self._conn.execute("DELETE FROM notes_fts WHERE key = ?", (key,))
                self._conn.execute(
                    "INSERT INTO notes_fts(key, value) VALUES(?, ?)", (key, value)
                )
            self._conn.commit()
        await bus.publish(MemoryWritten(note_key=key, preview=value[:120]))

    async def read_note(self, key: str) -> str | None:
        async with self._lock:
            row = self._conn.execute(
                "SELECT value FROM notes WHERE key = ?", (key,)
            ).fetchone()
        return row["value"] if row else None

    # ────────────────────────── task history ──────────────────────────

    async def record_task(self, outcome: TaskOutcome) -> None:
        ts = _now()
        async with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO task_history "
                "(task_id, project_id, title, outcome, detail, completed_at) "
                "VALUES (?,?,?,?,?,?)",
                (
                    outcome.task_id,
                    outcome.project_id,
                    outcome.title,
                    outcome.outcome,
                    outcome.detail,
                    ts,
                ),
            )
            if self._has_fts:
                self._conn.execute(
                    "INSERT INTO history_fts(title, detail) VALUES(?, ?)",
                    (outcome.title, outcome.detail or ""),
                )
            self._conn.commit()

    # ────────────────────────── events ──────────────────────────

    async def record_event(self, event: Any) -> None:
        try:
            payload = event.model_dump()
        except AttributeError:
            return
        async with self._lock:
            self._conn.execute(
                "INSERT OR REPLACE INTO events(id, ts, type, agent_id, project_id, payload) "
                "VALUES(?,?,?,?,?,?)",
                (
                    payload.get("id"),
                    payload.get("ts"),
                    payload.get("type"),
                    payload.get("agent_id"),
                    payload.get("project_id"),
                    json.dumps(payload),
                ),
            )
            self._conn.commit()

    # ────────────────────────── recall ──────────────────────────

    async def recall(self, query: str, k: int = 5) -> list[str]:
        """Return up to ``k`` short snippets relevant to ``query``.

        Uses FTS5 if available, otherwise a LIKE fallback so the API stays
        valid on stripped-down SQLite builds. The interface stays stable so
        a future embedding-backed implementation can drop in here.
        """
        if not query.strip():
            return []
        async with self._lock:
            results: list[str] = []
            if self._has_fts:
                # FTS5 needs a query escape: wrap in quotes to treat as phrase.
                fts_q = '"' + query.replace('"', '""') + '"'
                for row in self._conn.execute(
                    "SELECT key, value FROM notes_fts WHERE notes_fts MATCH ? LIMIT ?",
                    (fts_q, k),
                ):
                    results.append(f"note[{row['key']}]: {row['value']}")
                if len(results) < k:
                    remaining = k - len(results)
                    for row in self._conn.execute(
                        "SELECT title, detail FROM history_fts WHERE history_fts MATCH ? LIMIT ?",
                        (fts_q, remaining),
                    ):
                        results.append(
                            f"history: {row['title']} — {row['detail'] or ''}"
                        )
            else:
                like = f"%{query}%"
                for row in self._conn.execute(
                    "SELECT key, value FROM notes WHERE value LIKE ? OR key LIKE ? LIMIT ?",
                    (like, like, k),
                ):
                    results.append(f"note[{row['key']}]: {row['value']}")
                if len(results) < k:
                    remaining = k - len(results)
                    for row in self._conn.execute(
                        "SELECT title, detail FROM task_history "
                        "WHERE title LIKE ? OR detail LIKE ? "
                        "ORDER BY completed_at DESC LIMIT ?",
                        (like, like, remaining),
                    ):
                        results.append(
                            f"history: {row['title']} — {row['detail'] or ''}"
                        )
        return results[:k]

    # ────────────────────────── lifecycle ──────────────────────────

    async def subscribe_bus(self) -> None:
        """Background task: persist every bus event to the events table."""
        async for event in bus.subscribe():
            try:
                await self.record_event(event)
            except Exception:
                # Never let a bad event take down the persistence task.
                continue

    def close(self) -> None:
        try:
            self._conn.close()
        except sqlite3.Error:
            pass


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
