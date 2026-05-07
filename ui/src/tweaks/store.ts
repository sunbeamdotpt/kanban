import { observable } from "@legendapp/state";
import { use$ } from "@legendapp/state/react";
import { useEffect } from "react";

/** Tweaks configuration shape. */
export interface Tweaks {
  theme: "light" | "dark";
  density: "cozy" | "compact";
  accents: boolean;
  showWip: boolean;
  groupBy: "status" | "assignee" | "priority";
}

/** Default tweaks configuration. */
export const DEFAULT_TWEAKS: Tweaks = {
  theme: "light",
  density: "cozy",
  accents: true,
  showWip: true,
  groupBy: "status",
};

const STORAGE_KEY = "kanban.tweaks";

function loadFromStorage(): Tweaks {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (!stored) return DEFAULT_TWEAKS;
    const parsed = JSON.parse(stored);
    return {
      theme: parsed.theme ?? DEFAULT_TWEAKS.theme,
      density: parsed.density ?? DEFAULT_TWEAKS.density,
      accents: parsed.accents ?? DEFAULT_TWEAKS.accents,
      showWip: parsed.showWip ?? DEFAULT_TWEAKS.showWip,
      groupBy: parsed.groupBy ?? DEFAULT_TWEAKS.groupBy,
    };
  } catch {
    return DEFAULT_TWEAKS;
  }
}

function saveToStorage(tweaks: Tweaks): void {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(tweaks));
  } catch {
    // localStorage unavailable; silently continue.
  }
}

/** Module-level singleton observable so every `useTweaks()` call shares state. */
const tweaksState$ = observable<Tweaks>(DEFAULT_TWEAKS);
const hydrated$ = observable(false);
let hydrateInitiated = false;

function hydrateOnce() {
  if (hydrateInitiated) return;
  hydrateInitiated = true;
  tweaksState$.set(loadFromStorage());
  hydrated$.set(true);
}

export function useTweaks() {
  // Trigger hydrate exactly once across all consumers.
  useEffect(() => {
    hydrateOnce();
  }, []);

  const tweaks = use$(tweaksState$);
  const hydrated = use$(hydrated$);

  const setTweak = <K extends keyof Tweaks>(key: K, value: Tweaks[K]) => {
    const updated = { ...tweaksState$.get(), [key]: value };
    tweaksState$.set(updated);
    saveToStorage(updated);
  };

  const reset = () => {
    tweaksState$.set(DEFAULT_TWEAKS);
    saveToStorage(DEFAULT_TWEAKS);
  };

  return { tweaks, setTweak, reset, hydrated };
}

/** Test-only: reset module state between specs. */
export function __resetTweaksStore() {
  hydrateInitiated = false;
  tweaksState$.set(DEFAULT_TWEAKS);
  hydrated$.set(false);
}
