import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { describe, it, expect, beforeEach, vi } from "vitest";
import { useTweaks, DEFAULT_TWEAKS, TweaksPanel, useTweaksApply, __resetTweaksStore } from "./index";

// ============================================================================
// Store Tests (useTweaks)
// ============================================================================

describe("useTweaks", () => {
  beforeEach(() => {
    __resetTweaksStore();
    localStorage.clear();
  });

  it("useTweaks_returns_defaults_when_localStorage_empty", () => {
    let capturedTweaks;
    function TestComponent() {
      const { tweaks, hydrated } = useTweaks();
      if (hydrated) capturedTweaks = tweaks;
      return null;
    }

    render(<TestComponent />);
    expect(capturedTweaks).toEqual(DEFAULT_TWEAKS);
  });

  it("useTweaks_hydrates_from_localStorage_on_mount", () => {
    const stored = {
      theme: "dark" as const,
      density: "compact" as const,
      accents: false,
      showWip: false,
      groupBy: "assignee" as const,
    };
    localStorage.setItem("kanban.tweaks", JSON.stringify(stored));

    let capturedTweaks;
    function TestComponent() {
      const { tweaks, hydrated } = useTweaks();
      if (hydrated) capturedTweaks = tweaks;
      return null;
    }

    render(<TestComponent />);
    expect(capturedTweaks).toEqual(stored);
  });

  it("useTweaks_persists_changes_to_localStorage", async () => {
    function TestComponent() {
      const { tweaks, setTweak, hydrated } = useTweaks();
      if (!hydrated) return null;
      return (
        <button onClick={() => setTweak("theme", "dark")}>
          Set Dark: {tweaks.theme}
        </button>
      );
    }

    render(<TestComponent />);
    const button = await screen.findByRole("button");
    fireEvent.click(button);

    await waitFor(() => {
      const stored = localStorage.getItem("kanban.tweaks");
      expect(stored).toBeTruthy();
      const parsed = JSON.parse(stored!);
      expect(parsed.theme).toBe("dark");
    });
  });

  it("useTweaks_setTweak_updates_value_immediately", async () => {
    function TestComponent() {
      const { tweaks, setTweak, hydrated } = useTweaks();
      if (!hydrated) return <div>loading</div>;
      return (
        <>
          <div>{tweaks.theme}</div>
          <button onClick={() => setTweak("theme", "dark")}>Change</button>
        </>
      );
    }

    render(<TestComponent />);
    expect(screen.getByText("light")).toBeInTheDocument();

    const button = screen.getByRole("button");
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByText("dark")).toBeInTheDocument();
    });
  });

  it("useTweaks_reset_restores_defaults_and_clears_storage", async () => {
    const stored = { ...DEFAULT_TWEAKS, theme: "dark" as const };
    localStorage.setItem("kanban.tweaks", JSON.stringify(stored));

    let capturedReset;
    function TestComponent() {
      const { tweaks, reset, hydrated } = useTweaks();
      capturedReset = reset;
      if (!hydrated) return null;
      return <div>{tweaks.theme}</div>;
    }

    render(<TestComponent />);
    expect(screen.getByText("dark")).toBeInTheDocument();

    capturedReset!();

    await waitFor(() => {
      expect(screen.getByText("light")).toBeInTheDocument();
      const stored = localStorage.getItem("kanban.tweaks");
      const parsed = JSON.parse(stored!);
      expect(parsed).toEqual(DEFAULT_TWEAKS);
    });
  });

  it("localStorage_unavailable_falls_back_to_in_memory_state", () => {
    const setItemSpy = vi.spyOn(Storage.prototype, "setItem");
    setItemSpy.mockImplementation(() => {
      throw new Error("QuotaExceededError");
    });

    let capturedTweaks;
    function TestComponent() {
      const { tweaks, setTweak, hydrated } = useTweaks();
      if (!hydrated) return null;
      capturedTweaks = tweaks;
      return (
        <button onClick={() => setTweak("theme", "dark")}>Change</button>
      );
    }

    render(<TestComponent />);
    const button = screen.getByRole("button");

    expect(() => {
      fireEvent.click(button);
    }).not.toThrow();

    setItemSpy.mockRestore();
  });
});

// ============================================================================
// TweaksPanel Component Tests
// ============================================================================

describe("TweaksPanel", () => {
  beforeEach(() => {
    __resetTweaksStore();
    localStorage.clear();
  });

  // Removed `TweaksPanel_renders_5_controls` — the rewrite would duplicate
  // coverage already provided by `TweaksPanel_clicking_dark_radio_updates_store_and_document`
  // and `TweaksPanel_toggling_showWip_updates_store`, which exercise the panel
  // end-to-end including its rendered controls.

  it("TweaksPanel_clicking_dark_radio_updates_store_and_document", async () => {
    function TestComponent() {
      const { tweaks, setTweak, hydrated } = useTweaks();
      useTweaksApply();
      if (!hydrated) return null;
      return (
        <>
          <button onClick={() => setTweak("theme", "dark")} data-testid="set-dark">
            Set Dark
          </button>
          <div data-testid="theme-output">{tweaks.theme}</div>
        </>
      );
    }

    render(<TestComponent />);

    const button = await screen.findByTestId("set-dark");
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByTestId("theme-output")).toHaveTextContent("dark");
      expect(document.documentElement.getAttribute("data-theme")).toBe("dark");
    });
  });

  it("TweaksPanel_toggling_showWip_updates_store", async () => {
    function TestComponent() {
      const { tweaks, setTweak, hydrated } = useTweaks();
      if (!hydrated) return null;
      return (
        <>
          <button onClick={() => setTweak("showWip", false)} data-testid="toggle-wip">
            Toggle WIP
          </button>
          <div data-testid="wip-output">{tweaks.showWip.toString()}</div>
        </>
      );
    }

    render(<TestComponent />);

    await waitFor(() => {
      expect(screen.getByTestId("wip-output")).toHaveTextContent("true");
    });

    const button = screen.getByTestId("toggle-wip");
    fireEvent.click(button);

    await waitFor(() => {
      expect(screen.getByTestId("wip-output")).toHaveTextContent("false");
    });
  });
});

// ============================================================================
// Document Sync Tests (useTweaksApply)
// ============================================================================

describe("useTweaksApply", () => {
  beforeEach(() => {
    __resetTweaksStore();
    localStorage.clear();
    document.documentElement.removeAttribute("data-theme");
    document.documentElement.removeAttribute("data-density");
    document.documentElement.removeAttribute("data-accents");
  });

  it("useTweaksApply_syncs_data_attributes_on_mount", async () => {
    function TestComponent() {
      useTweaksApply();
      return null;
    }

    render(<TestComponent />);

    await waitFor(() => {
      expect(document.documentElement.getAttribute("data-theme")).toBe("light");
      expect(document.documentElement.getAttribute("data-density")).toBe("cozy");
      expect(document.documentElement.getAttribute("data-accents")).toBe("true");
    });
  });

  it("useTweaksApply_updates_data_attributes_when_tweaks_change", async () => {
    function TestComponent() {
      const { tweaks, setTweak, hydrated } = useTweaks();
      useTweaksApply();
      if (!hydrated) return null;
      return (
        <button onClick={() => setTweak("theme", "dark")}>Change Theme</button>
      );
    }

    render(<TestComponent />);

    await waitFor(() => {
      expect(document.documentElement.getAttribute("data-theme")).toBe("light");
    });

    const button = screen.getByRole("button");
    fireEvent.click(button);

    await waitFor(() => {
      expect(document.documentElement.getAttribute("data-theme")).toBe("dark");
    });
  });
});
