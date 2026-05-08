"use client";
import { useEventStream } from "@/lib/useEventStream";
import { useStore } from "@/lib/store";
import { AgentCard } from "@/components/AgentCard";
import { GoalInput } from "@/components/GoalInput";

export default function Dashboard() {
  useEventStream();
  const agents = useStore((s) => Object.values(s.agents));

  const grouped = {
    active: agents.filter((a) => a.status === "running" || a.status === "pending"),
    reviewing: agents.filter((a) => a.status === "reviewing"),
    done: agents.filter((a) =>
      ["merged", "failed", "killed"].includes(a.status)
    ),
  };

  return (
    <main className="min-h-screen flex flex-col">
      <header className="border-b border-zinc-800 px-6 py-4 flex items-center justify-between">
        <h1 className="text-lg font-semibold tracking-tight">
          AI Overseer <span className="text-zinc-500 font-normal">·  multi-agent control</span>
        </h1>
        <div className="text-xs text-zinc-500">
          {agents.length} agent{agents.length === 1 ? "" : "s"}
        </div>
      </header>

      <div className="px-6 py-4 border-b border-zinc-800">
        <GoalInput />
      </div>

      <div className="flex-1 grid grid-cols-3 gap-4 p-4 min-h-0">
        <Column title="Active" agents={grouped.active} accent="text-blue-400" />
        <Column title="Awaiting review" agents={grouped.reviewing} accent="text-amber-400" />
        <Column title="Finished" agents={grouped.done} accent="text-zinc-500" />
      </div>
    </main>
  );
}

function Column({
  title,
  agents,
  accent,
}: {
  title: string;
  agents: ReturnType<typeof useStore.getState>["agents"][string][];
  accent: string;
}) {
  return (
    <section className="flex flex-col min-h-0">
      <h2 className={`text-xs uppercase tracking-wider mb-2 ${accent}`}>
        {title} · {agents.length}
      </h2>
      <div className="flex-1 overflow-y-auto pr-1 space-y-3">
        {agents.length === 0 && (
          <div className="text-xs text-zinc-600 italic">No agents</div>
        )}
        {agents.map((a) => (
          <AgentCard key={a.agentId} agent={a} />
        ))}
      </div>
    </section>
  );
}
