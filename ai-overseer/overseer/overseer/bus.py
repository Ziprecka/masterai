"""In-process pub-sub event bus.

Hooks and orchestrator code call `bus.publish(event)`. The WebSocket
endpoint subscribes and forwards every event to connected clients.

Swap for Redis pub-sub when you need multi-worker scaling.
"""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from .events import Event


class EventBus:
    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue[Event]] = set()
        self._history: list[Event] = []
        self._max_history = 1000

    async def publish(self, event: Event) -> None:
        self._history.append(event)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history :]
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                # Slow subscriber — drop oldest by recreating
                pass

    async def subscribe(self) -> AsyncIterator[Event]:
        q: asyncio.Queue[Event] = asyncio.Queue(maxsize=500)
        self._subscribers.add(q)
        try:
            # Replay recent history to new subscribers
            for ev in self._history[-100:]:
                await q.put(ev)
            while True:
                yield await q.get()
        finally:
            self._subscribers.discard(q)


bus = EventBus()
