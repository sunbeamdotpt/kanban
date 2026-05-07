import { useEffect } from "react";
import { useTweaks } from "./store";

/**
 * Hook: useTweaksApply
 *
 * Syncs tweaks to document.documentElement attributes.
 * Call once near the app root (e.g., in a <Provider> wrapper).
 *
 * Sets:
 * - `data-theme`: "light" | "dark" (read by beam-ui presets)
 * - `data-density`: "cozy" | "compact"
 * - `data-accents`: "true" | "false"
 *
 * @example
 * ```tsx
 * function Root() {
 *   useTweaksApply();
 *   return <App />;
 * }
 * ```
 */
export function useTweaksApply(): void {
  const { tweaks, hydrated } = useTweaks();

  useEffect(() => {
    if (!hydrated) return;

    const root = document.documentElement;
    root.setAttribute("data-theme", tweaks.theme);
    root.setAttribute("data-density", tweaks.density);
    root.setAttribute("data-accents", tweaks.accents ? "true" : "false");
  }, [tweaks, hydrated]);
}
