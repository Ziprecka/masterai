"use client";
import { AgentState } from "@/lib/store";

const API = process.env.NEXT_PUBLIC_OVERSEER_API ?? "http://127.0.0.1:8000";

const STATUS_COLORS: Record<AgentState["status"], string> = {
  pending: "bg-zinc-700",
  running: "bg-blue-600 animate-pulse",
  reviewing: "bg-amber-600",
  merged: "bg-emerald-600",
  failed: "bg-red-700",
  killed: "bg-zinc-600",
};

export function AgentCard({ agent }: { agent: AgentState }) {
  async function action(verb: "kill" | "merge" | "discard") {
    await fetch(`${API}/api/agents/${agent.agentId}/${verb}`, { method: "POST" });
  }

  return (
    <div className="rounded-lg border border-zinc-800 bg-zinc-900/60 p-3 flex flex-col gap-2 min-h-0">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="text-xs text-zinc-500 font-mono">{agent.agentId}</div>
          <div className="font-medium truncate">{agent.task || "—"}</div>
          <div className="text-xs text-zinc-500 font-mono truncate">{agent.branch}</div>
        </div>
        <span
          className={`text-[10px] uppercase tracking-wider px-2 py-1 rounded ${STATUS_COLORS[agent.status]}`}
        >
          {agent.status}
        </span>
      </div>

      <div className="text-xs text-zinc-400 flex gap-3">
        <span>📝 {agent.filesChanged.size} files</span>
        <span>↑ {agent.inputTokens.toLocaleString()}</span>
        <span>↓ {agent.outputTokens.toLocaleString()}</span>
        <span>${agent.costUsd.toFixed(3)}</span>
      </div>

      <div className="bg-black/40 rounded p-2 text-xs font-mono h-48 overflow-y-auto flex flex-col-reverse">
        <div>
          {agent.log.slice().reverse().map((l) => (
            <div key={l.id} className={logColor(l.kind, l.isError)}>
              <span className="text-zinc-600">{kindGlyph(l.kind)}</span>{" "}
              <span>{l.text.slice(0, 220)}</span>
            </div>
          ))}
        </div>
      </div>

      <div className="flex gap-2">
        {agent.status === "reviewing" && (
          <>
            <button onClick={() => action("merge")}
              className="text-xs bg-emerald-700 hover:bg-emerald-600 px-2 py-1 rounded">
              Merge
            </button>
            <button onClick={() => action("discard")}
              className="text-xs bg-zinc-700 hover:bg-zinc-600 px-2 py-1 rounded">
              Discard
            </button>
          </>
        )}
        {(agent.status === "running" || agent.status === "pending") && (
          <button onClick={() => action("kill")}
            className="text-xs bg-red-800 hover:bg-red-700 px-2 py-1 rounded">
            Kill
          </button>
        )}
      </div>
    </div>
  );
}

function kindGlyph(k: string): string {
  return ({
    tool_call: "▸",
    tool_result: "◂",
    agent_message: "💬",
    file_changed: "📄",
    status: "●",
  } as Record<string, string>)[k] ?? "·";
}

function logColor(k: string, isError?: boolean): string {
  if (isError) return "text-red-400";
  return ({
    tool_call: "text-blue-300",
    tool_result: "text-zinc-400",
    agent_message: "text-emerald-300",
    file_changed: "text-amber-300",
    status: "text-purple-300",
  } as Record<string, string>)[k] ?? "text-zinc-400";
}
