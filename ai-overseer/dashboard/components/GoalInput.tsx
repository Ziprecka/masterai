"use client";
import { useState } from "react";

const API = process.env.NEXT_PUBLIC_OVERSEER_API ?? "http://127.0.0.1:8000";

export function GoalInput() {
  const [goal, setGoal] = useState("");
  const [submitting, setSubmitting] = useState(false);

  async function submit() {
    if (!goal.trim() || submitting) return;
    setSubmitting(true);
    try {
      await fetch(`${API}/api/goals`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ goal, auto_spawn: true }),
      });
      setGoal("");
    } finally {
      setSubmitting(false);
    }
  }

  return (
    <div className="flex gap-2">
      <input
        className="flex-1 bg-zinc-900 border border-zinc-800 rounded px-3 py-2 text-sm focus:outline-none focus:border-zinc-600"
        placeholder="What should the overseer build, refactor, or fix?"
        value={goal}
        onChange={(e) => setGoal(e.target.value)}
        onKeyDown={(e) => e.key === "Enter" && submit()}
      />
      <button
        onClick={submit}
        disabled={submitting || !goal.trim()}
        className="bg-blue-600 hover:bg-blue-500 disabled:opacity-50 px-4 py-2 rounded text-sm font-medium"
      >
        {submitting ? "Planning…" : "Dispatch"}
      </button>
    </div>
  );
}
