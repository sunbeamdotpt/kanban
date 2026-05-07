/**
 * Kanban Connect transport factory.
 * Wires createTransport from @sunbeam/g2v/core with the withAuth interceptor
 * reading from authStore.
 */

import { useMemo } from "react";
import { type Transport } from "@connectrpc/connect";
import { createTransport } from "@sunbeam/g2v/core";
import { withAuth } from "@sunbeam/g2v/interceptors";
import { authStore } from "@sunbeam/g2v/state";

/**
 * Create a Connect transport for the kanban API.
 * Automatically attaches the current bearer token from authStore to every request.
 */
export function createKanbanTransport(): Transport {
  return createTransport({
    baseUrl: (import.meta.env.VITE_KANBAN_API_URL as string | undefined) ?? "/api",
    interceptors: [
      withAuth({
        getToken: () => authStore.session.get()?.accessToken ?? undefined,
      }),
    ],
  });
}

/**
 * React hook that memoizes the kanban transport for the component lifetime.
 * Re-creates the transport if the component remounts (stable across renders).
 */
export function useKanbanTransport(): Transport {
  // eslint-disable-next-line react-hooks/exhaustive-deps
  return useMemo(() => createKanbanTransport(), []);
}
