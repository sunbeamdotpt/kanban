/**
 * Dev-only design preview at /__preview.
 *
 * Side-by-side visual diff harness:
 *   left  iframe → /__ref-page.html  (frozen reference baseline)
 *   right iframe → /__live-page.html (rebuild-in-progress; edit freely)
 *
 * Both files live under apps/kanban/ui/public/. The right pane starts as a
 * copy of the left and is refactored incrementally toward beam-ui tokens
 * and components. Toggle ref / live / split via the toolbar.
 *
 * Excluded from prod build via routes/index.tsx (DEV-only route).
 */

import { useState } from "react";

type Mode = "ref" | "live" | "split";

export function PreviewPage() {
  const [mode, setMode] = useState<Mode>("split");

  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100vh",
        width: "100vw",
        background: "#fffaeb",
      }}
    >
      <div
        style={{
          display: "flex",
          gap: "8px",
          padding: "6px 12px",
          borderBottom: "1px solid #e8d9a8",
          background: "#fff0c2",
          fontFamily: "-apple-system, BlinkMacSystemFont, sans-serif",
          fontSize: "12px",
          alignItems: "center",
          flex: "0 0 auto",
        }}
      >
        <strong style={{ marginRight: "4px" }}>preview</strong>
        {(["ref", "split", "live"] as const).map((m) => (
          <button
            key={m}
            onClick={() => setMode(m)}
            style={{
              padding: "3px 10px",
              border: "1px solid #e8d9a8",
              background: mode === m ? "#fa520f" : "#fffaeb",
              color: mode === m ? "#fff" : "#333",
              borderRadius: "4px",
              cursor: "pointer",
              fontWeight: mode === m ? 600 : 400,
            }}
          >
            {m}
          </button>
        ))}
        <span style={{ marginLeft: "auto", color: "#888", fontSize: "11px" }}>
          left = /__ref-page.html · right = /__live-page.html
        </span>
      </div>
      <div style={{ display: "flex", flex: 1, minHeight: 0 }}>
        {(mode === "ref" || mode === "split") && (
          <iframe
            src="/__ref-page.html"
            title="reference"
            style={{
              flex: 1,
              border: "none",
              borderRight: mode === "split" ? "1px solid #e8d9a8" : "none",
              minWidth: 0,
            }}
          />
        )}
        {(mode === "live" || mode === "split") && (
          <iframe
            src="/__live-page.html"
            title="live"
            style={{ flex: 1, border: "none", minWidth: 0 }}
          />
        )}
      </div>
    </div>
  );
}
