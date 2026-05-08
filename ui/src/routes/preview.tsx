/**
 * Dev-only design preview at /__preview.
 * Renders KanbanBoard with hardcoded fixtures matching Sunbeam Kanban.html
 * so we can visually diff beam-ui rendering against the reference design.
 *
 * Excluded from prod build via routes/index.tsx (which only includes this
 * route under `import.meta.env.DEV`).
 */

import { useState } from "react";
import { KanbanBoard, type KanbanColumn } from "@sunbeam/beam-ui";

// Reference palette: muted pastel pills against cream/ivory bg.
const TAG = {
  medium: "#ffd06a",
  high: "#fb6424",
  refactor: "#fdc69a",
  UI: "#9ec5fe",
  feature: "#a3e6c2",
  perf: "#ffe295",
  design: "#cdb4f1",
  bug: "#fbb29a",
  ally: "#c4a3e8",
  critical: "#fa520f",
};

const COL_MEMBERS = [
  { name: "Mira Cho" },
  { name: "Bea Anderson" },
  { name: "Cy Lin" },
  { name: "Mei Li" },
  { name: "Asher Kim" },
  { name: "Riya Patel" },
];

const initialColumns: KanbanColumn[] = [
  {
    id: "up-next",
    title: "UP NEXT",
    accentColor: "#fa520f",
    wipLimit: 5,
    members: COL_MEMBERS.slice(0, 6),
    cards: [
      {
        id: "BEAM-210",
        shortId: "BEAM-210",
        title: "Refactor the Splitter component",
        labels: [
          { name: "medium", color: TAG.medium },
          { name: "refactor", color: TAG.refactor },
          { name: "UI", color: TAG.UI },
        ],
        checklist: { done: 2, total: 7 },
        milestone: "v2.0 Release",
        dueDate: "12 May",
        commentCount: 4,
        assignees: [{ name: "Bea Anderson" }],
      },
      {
        id: "BEAM-208",
        shortId: "BEAM-208",
        title: "Add color picker dark mode support",
        labels: [
          { name: "UI", color: TAG.UI },
          { name: "design", color: TAG.design },
        ],
        checklist: { done: 0, total: 3 },
        milestone: "v2.0 Release",
        dueDate: "—",
        commentCount: 2,
        attachmentCount: 1,
        assignees: [{ name: "Mira Sato" }],
        cover: "linear-gradient(135deg, #ffd06a 0%, #fa520f 100%)",
      },
    ],
  },
  {
    id: "in-progress",
    title: "IN PROGRESS",
    accentColor: "#fa520f",
    wipLimit: 3,
    members: COL_MEMBERS.slice(0, 6),
    cards: [
      {
        id: "BEAM-215",
        shortId: "BEAM-215",
        title: "Combobox: support virtualised options for 10k+ items",
        labels: [
          { name: "high", color: TAG.high },
          { name: "feature", color: TAG.feature },
          { name: "perf", color: TAG.perf },
        ],
        checklist: { done: 5, total: 9 },
        milestone: "v2.0 Release",
        dueDate: "8 May",
        commentCount: 6,
        assignees: [{ name: "Mira Cho" }, { name: "Cy Lin" }],
      },
      {
        id: "BEAM-207",
        shortId: "BEAM-207",
        title: "Fix modals closing on outside click during text selection",
        labels: [
          { name: "!critical", color: TAG.critical },
          { name: "bug", color: TAG.bug },
        ],
        checklist: { done: 1, total: 4 },
        milestone: "Hardening Sprint",
        dueDate: "3 May",
        commentCount: 3,
        assignees: [{ name: "Mei Li" }, { name: "Asher Kim" }],
      },
      {
        id: "BEAM-203",
        shortId: "BEAM-203",
        title: "Calendar: support week-start configuration",
        labels: [{ name: "feature", color: TAG.feature }],
        checklist: { done: 1, total: 4 },
        milestone: "v2.0 Release",
        dueDate: "—",
        commentCount: 1,
        assignees: [{ name: "Riya Patel" }],
      },
    ],
  },
  {
    id: "in-review",
    title: "IN REVIEW",
    accentColor: "#fa520f",
    wipLimit: 4,
    members: COL_MEMBERS.slice(0, 6),
    cards: [
      {
        id: "BEAM-216",
        shortId: "BEAM-216",
        title: "Toast component: API simplification",
        labels: [
          { name: "medium", color: TAG.medium },
          { name: "refactor", color: TAG.refactor },
          { name: "feature", color: TAG.feature },
        ],
        checklist: { done: 3, total: 3 },
        milestone: "v2.0 Release",
        dueDate: "—",
        commentCount: 1,
        assignees: [{ name: "Sol Reyes" }],
      },
      {
        id: "BEAM-211",
        shortId: "BEAM-211",
        title: "Dialog focus trap regression",
        labels: [
          { name: "high", color: TAG.high },
          { name: "bug", color: TAG.bug },
          { name: "ally", color: TAG.ally },
        ],
        milestone: "Hardening Sprint",
        dueDate: "—",
        assignees: [{ name: "Avi Brooks" }],
        blocked: true,
      },
    ],
  },
  {
    id: "done",
    title: "DONE",
    accentColor: "#fa520f",
    members: COL_MEMBERS.slice(0, 6),
    cards: [
      {
        id: "BEAM-201",
        shortId: "BEAM-201",
        title: "Spinner — replace with sinusoidal worm motion",
        labels: [{ name: "design", color: TAG.design }],
        checklist: { done: 5, total: 5 },
        milestone: "v2.0 Release",
        dueDate: "28 Apr",
        commentCount: 2,
        assignees: [{ name: "Mira Cho" }],
      },
      {
        id: "BEAM-199",
        shortId: "BEAM-199",
        title: "Avatar stack: support overflow indicator",
        labels: [{ name: "feature", color: TAG.feature }],
        checklist: { done: 4, total: 4 },
        milestone: "v2.0 Release",
        dueDate: "26 Apr",
        commentCount: 1,
        assignees: [{ name: "Bea Anderson" }],
      },
    ],
  },
];

export function PreviewPage() {
  const [columns, setColumns] = useState(initialColumns);

  return (
    <div
      style={{
        background: "#fffaeb",
        minHeight: "100vh",
        display: "flex",
        flexDirection: "column",
        fontFamily:
          "'Ysabeau Infant', -apple-system, BlinkMacSystemFont, sans-serif",
      }}
    >
      {/* Top bar */}
      <header
        style={{
          display: "flex",
          alignItems: "center",
          gap: "24px",
          padding: "12px 24px",
          borderBottom: "1px solid rgba(127,99,21,0.15)",
          background: "rgba(255,250,235,0.92)",
        }}
      >
        <div
          style={{
            display: "flex",
            alignItems: "center",
            gap: "8px",
            fontWeight: 791,
          }}
        >
          <span style={{ color: "#fa520f", fontSize: "20px" }}>✦</span>
          <span style={{ fontSize: "13px" }}>Sunbeam Kanban</span>
        </div>
        <div
          style={{
            color: "#fa520f",
            fontSize: "11px",
            fontWeight: 791,
            letterSpacing: "0.1em",
            textTransform: "uppercase",
          }}
        >
          BEAM UI / COMPONENTS
        </div>
        <div style={{ flex: 1 }}>
          <div
            style={{
              maxWidth: "440px",
              marginLeft: "auto",
              marginRight: "120px",
              display: "flex",
              alignItems: "center",
              gap: "8px",
              padding: "6px 12px",
              background: "#fff0c2",
              border: "1px solid rgba(127,99,21,0.15)",
              borderRadius: "0",
              fontSize: "12px",
              color: "#7f6315",
            }}
          >
            <span>🔍</span>
            <span style={{ flex: 1 }}>Jump to color, board, or person…</span>
            <span
              style={{
                fontFamily: "'Monaspace Argon', monospace",
                fontSize: "10px",
                background: "#fffaeb",
                padding: "1px 6px",
                border: "1px solid rgba(127,99,21,0.15)",
              }}
            >
              ⌘K
            </span>
          </div>
        </div>
        <div style={{ display: "flex", alignItems: "center", gap: "12px" }}>
          <span style={{ color: "#7f6315", fontSize: "16px" }}>🔔</span>
          <div
            style={{
              width: "28px",
              height: "28px",
              borderRadius: "50%",
              background: "#fa520f",
              color: "white",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              fontSize: "11px",
              fontWeight: 791,
            }}
          >
            SP
          </div>
        </div>
      </header>

      {/* Body: sidebar + main */}
      <div style={{ flex: 1, display: "flex", overflow: "hidden" }}>
        {/* Sidebar */}
        <aside
          style={{
            width: "240px",
            borderRight: "1px solid rgba(127,99,21,0.15)",
            padding: "16px 12px",
            fontSize: "12px",
            color: "#1f1f1f",
          }}
        >
          <div
            style={{
              display: "flex",
              alignItems: "center",
              justifyContent: "space-between",
              padding: "0 4px 8px",
              borderBottom: "1px solid rgba(127,99,21,0.08)",
              marginBottom: "10px",
            }}
          >
            <span
              style={{
                fontSize: "10px",
                color: "#7f6315",
                fontWeight: 791,
                letterSpacing: "0.1em",
              }}
            >
              WORKSPACE
            </span>
            <span style={{ color: "#7f6315" }}>+</span>
          </div>
          {[
            {
              name: "Beam UI",
              count: 2,
              color: "#1abc9c",
              icon: "B",
              boards: [
                { name: "Components", n: 13, active: true },
                { name: "v2 Migration", n: 4 },
              ],
            },
            {
              name: "Sol",
              count: 2,
              color: "#fa520f",
              icon: "S",
              boards: [
                { name: "Roadmap", n: 6 },
                { name: "Bugs", n: 3 },
              ],
            },
            {
              name: "Marathon",
              count: 1,
              color: "#7c3aed",
              icon: "M",
              boards: [{ name: "Engine", n: 7 }],
            },
            {
              name: "Studios Site",
              count: 1,
              color: "#fb6424",
              icon: "S",
              boards: [{ name: "Content", n: 2 }],
            },
          ].map((proj) => (
            <div key={proj.name} style={{ marginBottom: "12px" }}>
              <div
                style={{
                  display: "flex",
                  alignItems: "center",
                  gap: "8px",
                  padding: "4px 6px",
                }}
              >
                <span style={{ color: "#7f6315", fontSize: "10px" }}>▾</span>
                <div
                  style={{
                    width: "20px",
                    height: "20px",
                    background: proj.color,
                    color: "white",
                    fontSize: "11px",
                    fontWeight: 791,
                    display: "flex",
                    alignItems: "center",
                    justifyContent: "center",
                  }}
                >
                  {proj.icon}
                </div>
                <span style={{ flex: 1 }}>{proj.name}</span>
                <span style={{ color: "#7f6315", fontSize: "11px" }}>
                  {proj.count}
                </span>
              </div>
              <div style={{ paddingLeft: "32px" }}>
                {proj.boards.map((b) => (
                  <div
                    key={b.name}
                    style={{
                      display: "flex",
                      alignItems: "center",
                      gap: "6px",
                      padding: "3px 4px",
                      color: b.active ? "#fa520f" : "#1f1f1f",
                      fontWeight: b.active ? 791 : 647,
                    }}
                  >
                    {b.active && (
                      <span style={{ color: "#fa520f" }}>●</span>
                    )}
                    <span style={{ flex: 1 }}>{b.name}</span>
                    <span
                      style={{
                        color: "#7f6315",
                        fontSize: "10px",
                        fontFamily: "monospace",
                      }}
                    >
                      {b.n}
                    </span>
                  </div>
                ))}
              </div>
            </div>
          ))}
        </aside>

        {/* Main */}
        <main style={{ flex: 1, overflow: "auto" }}>
          <div style={{ padding: "16px 24px" }}>
            <div
              style={{
                display: "flex",
                alignItems: "center",
                justifyContent: "space-between",
                marginBottom: "16px",
              }}
            >
              <p style={{ color: "#7f6315", fontSize: "13px", margin: 0 }}>
                Day-to-day work on the component catalogue — new primitives,
                fixes, accessibility passes.
              </p>
              <div style={{ display: "flex", gap: "10px" }}>
                <button
                  type="button"
                  style={{
                    padding: "6px 14px",
                    background: "transparent",
                    border: "1px solid rgba(127,99,21,0.15)",
                    color: "#1f1f1f",
                    fontSize: "11px",
                    fontWeight: 791,
                    letterSpacing: "0.06em",
                  }}
                >
                  ↗ SHARE
                </button>
                <button
                  type="button"
                  style={{
                    padding: "6px 14px",
                    background: "#fa520f",
                    border: "1px solid #fa520f",
                    color: "white",
                    fontSize: "11px",
                    fontWeight: 791,
                    letterSpacing: "0.06em",
                  }}
                >
                  + NEW COLUMN
                </button>
              </div>
            </div>

            <FilterBar />
            <KanbanBoard
              columns={columns}
              onChange={setColumns}
              onAddCard={() => undefined}
            />
          </div>
        </main>
      </div>
    </div>
  );
}

function FilterBar() {
  const cellBase: React.CSSProperties = {
    fontSize: "10px",
    fontWeight: 791,
    letterSpacing: "0.08em",
    textTransform: "uppercase",
    padding: "5px 10px",
    background: "transparent",
    border: "1px solid rgba(127,99,21,0.15)",
    color: "#1f1f1f",
    cursor: "pointer",
  };
  const activeCell: React.CSSProperties = {
    ...cellBase,
    background: "#fa520f",
    color: "#fff",
    borderColor: "#fa520f",
  };
  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        gap: "6px",
        marginBottom: "12px",
        flexWrap: "wrap",
      }}
    >
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "6px",
          padding: "4px 10px",
          border: "1px solid rgba(127,99,21,0.15)",
          background: "#fff0c2",
          fontSize: "11px",
          color: "#7f6315",
          minWidth: "180px",
        }}
      >
        <span>🔍</span>
        <span>Filter cards…</span>
      </div>
      <button type="button" style={cellBase}>▦ LABEL</button>
      <button type="button" style={cellBase}>⌗ FILTER</button>
      <span
        style={{
          fontSize: "10px",
          color: "#7f6315",
          letterSpacing: "0.05em",
          marginLeft: "8px",
          marginRight: "4px",
          textTransform: "uppercase",
          fontWeight: 791,
        }}
      >
        Group by
      </span>
      <button type="button" style={activeCell}>STATUS</button>
      <button type="button" style={cellBase}>ASSIGNEE</button>
      <button type="button" style={cellBase}>PRIORITY</button>
      <div style={{ flex: 1 }} />
      <button type="button" style={activeCell}>▦ BOARD</button>
      <button type="button" style={cellBase}>≡ LIST</button>
      <div
        style={{
          display: "flex",
          marginLeft: "8px",
        }}
      >
        {["MC", "BA", "CL", "ML", "AK", "RP"].map((initials, i) => (
          <div
            key={initials}
            style={{
              width: "22px",
              height: "22px",
              borderRadius: "50%",
              background: ["#fa520f", "#1abc9c", "#7c3aed", "#fb6424", "#ffb83e", "#9ec5fe"][i % 6],
              color: "white",
              fontSize: "9px",
              fontWeight: 791,
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              border: "2px solid #fffaeb",
              marginRight: "-6px",
            }}
          >
            {initials}
          </div>
        ))}
      </div>
    </div>
  );
}
