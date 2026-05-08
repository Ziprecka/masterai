import { create } from "zustand";

export type AgentStatus =
  | "pending" | "running" | "reviewing" | "merged" | "failed" | "killed";

export interface LogEntry {
  id: string;
  ts: string;
  kind: "tool_call" | "tool_result" | "agent_message" | "file_changed" | "status";
  text: string;
  isError?: boolean;
}

export interface AgentState {
  agentId: string;
  task: string;
  branch: string;
  worktreePath: string;
  status: AgentStatus;
  inputTokens: number;
  outputTokens: number;
  costUsd: number;
  filesChanged: Set<string>;
  log: LogEntry[];
  finalMessage?: string;
  diffSummary?: string;
}

interface Store {
  agents: Record<string, AgentState>;
  apply: (event: any) => void;
  reset: () => void;
}

const MAX_LOG = 200;

export const useStore = create<Store>((set) => ({
  agents: {},
  reset: () => set({ agents: {} }),
  apply: (e) =>
    set((s) => {
      const id = e.agent_id;
      const prev = s.agents[id];
      const base: AgentState =
        prev ?? {
          agentId: id,
          task: "",
          branch: "",
          worktreePath: "",
          status: "pending",
          inputTokens: 0,
          outputTokens: 0,
          costUsd: 0,
          filesChanged: new Set(),
          log: [],
        };
      const log = base.log;
      const push = (entry: Omit<LogEntry, "id" | "ts">) => {
        const next = [
          ...log,
          { id: e.id, ts: e.ts, ...entry } as LogEntry,
        ];
        return next.length > MAX_LOG ? next.slice(-MAX_LOG) : next;
      };

      let next = { ...base };
      switch (e.type) {
        case "agent_spawned":
          next = {
            ...base,
            task: e.task,
            branch: e.branch,
            worktreePath: e.worktree_path,
            status: "pending",
          };
          break;
        case "agent_status":
          next = {
            ...base,
            status: e.status,
            log: push({ kind: "status", text: `→ ${e.status}${e.detail ? `: ${e.detail}` : ""}` }),
          };
          break;
        case "tool_call":
          next = {
            ...base,
            log: push({ kind: "tool_call", text: `${e.tool} ${shortInput(e.input)}` }),
          };
          break;
        case "tool_result":
          next = {
            ...base,
            log: push({
              kind: "tool_result",
              text: e.output_preview,
              isError: e.is_error,
            }),
          };
          break;
        case "agent_message":
          next = {
            ...base,
            log: push({ kind: "agent_message", text: e.text }),
          };
          break;
        case "file_changed": {
          const files = new Set(base.filesChanged);
          files.add(e.path);
          next = {
            ...base,
            filesChanged: files,
            log: push({ kind: "file_changed", text: `${e.change}: ${e.path}` }),
          };
          break;
        }
        case "token_usage":
          next = {
            ...base,
            inputTokens: e.input_tokens,
            outputTokens: e.output_tokens,
            costUsd: e.cumulative_cost_usd,
          };
          break;
        case "agent_finished":
          next = {
            ...base,
            finalMessage: e.final_message,
            diffSummary: e.diff_summary,
          };
          break;
        // New event types from the multi-project / autonomy / watcher
        // upgrade. UI handling lands in a follow-up; for now we just
        // accept them so the WS stream stays exhaustively switched.
        case "project_registered":
        case "circuit_state_changed":
        case "budget_exceeded":
        case "retry_scheduled":
        case "verification_started":
        case "verification_finished":
        case "watcher_registered":
        case "watcher_fired":
        case "conversation_turn":
        case "memory_written":
          break;
      }
      return { agents: { ...s.agents, [id]: next } };
    }),
}));

function shortInput(input: Record<string, unknown>): string {
  const keys = Object.keys(input);
  if (keys.length === 0) return "";
  const first = keys[0];
  return `${first}=${String(input[first]).slice(0, 60)}`;
}
