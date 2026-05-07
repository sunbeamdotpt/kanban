/**
 * Thin bridge from authStore (legend-state observable) to React state.
 * g2v-fe does not export useAuthStatus directly, so we expose it here.
 */

import { useSelector } from "@legendapp/state/react";
import { authStore, type AuthState } from "@sunbeam/g2v/state";

/** React hook that returns the current auth status, re-rendering on change. */
export function useAuthStatus(): AuthState["status"] {
  return useSelector(() => authStore.status.get());
}
