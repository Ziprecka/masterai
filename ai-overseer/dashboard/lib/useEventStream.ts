"use client";
import { useEffect, useRef } from "react";
import { useStore } from "./store";

const WS_URL =
  process.env.NEXT_PUBLIC_OVERSEER_WS ?? "ws://127.0.0.1:8000/ws";

export function useEventStream() {
  const apply = useStore((s) => s.apply);
  const wsRef = useRef<WebSocket | null>(null);

  useEffect(() => {
    let cancelled = false;
    let backoff = 500;

    function connect() {
      if (cancelled) return;
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;
      ws.onmessage = (m) => {
        try {
          apply(JSON.parse(m.data));
        } catch {
          /* ignore malformed */
        }
      };
      ws.onopen = () => {
        backoff = 500;
      };
      ws.onclose = () => {
        if (cancelled) return;
        setTimeout(connect, backoff);
        backoff = Math.min(backoff * 2, 8000);
      };
      ws.onerror = () => ws.close();
    }
    connect();
    return () => {
      cancelled = true;
      wsRef.current?.close();
    };
  }, [apply]);
}
