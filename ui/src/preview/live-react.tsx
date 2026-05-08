/**
 * LiveReact — the right pane of the /__preview visual diff.
 *
 * Renders the reference body markup verbatim (parsed via DOMParser) with
 * the reference CSS injected as a scoped style block. This makes the
 * right pane visually identical to /__ref-page.html on the left.
 *
 * The reference CSS uses `100vh` in `.app` / `.col` which assume a
 * full-viewport context. Inside our split harness LiveReact lives in a
 * flex pane whose height is `100vh - toolbar`. We override those rules
 * for descendants of `.preview-host` so they size against the host.
 *
 * Iteration plan: extract sections of ref-body.html into real React /
 * beam-ui components one block at a time. Replace the static block in
 * ref-body.html with a placeholder marker like
 *   <div data-react-slot="topbar"></div>
 * and mount the React component into the matching slot via portal.
 *
 * Both ref-styles.css and ref-body.html are extracted from the reference
 * page via the playwright dump (see public/__ref-page.html for source).
 */

import { useEffect, useRef, useState } from "react";
import { KanbanCardDetail, type KanbanCardData } from "@sunbeam/beam-ui/components/ui/kanban-card-detail";
import refCss from "./ref-styles.css?raw";
import refBody from "./ref-body.html?raw";

const SCOPE_OVERRIDES = `
.preview-host { position: absolute; inset: 0; width: 100%; height: 100%; min-width: 0; min-height: 0; }
.preview-host > .app { width: 100%; height: 100%; max-width: 100%; }
.preview-host .col { max-height: 100%; flex: 1 1 0; min-width: 0; max-width: 320px; width: auto; }
.preview-host .col.col--add { display: none; }
.preview-host .board { width: 100%; }
`;

const beam204: KanbanCardData = {
  id: "beam-204",
  shortId: "BEAM-204",
  breadcrumb: "Beam UI / Components",
  columnTitle: "Backlog",
  title: "Implement live-reload toggle for the showcase",
  labels: [
    { name: "feature", color: "orange" },
    { name: "design", color: "gold" },
  ],
  assignees: [{ name: "Sofia Pereira" }],
  milestone: "v2.0 Release",
  priority: "medium",
  checklist: [
    { id: "c1", title: "Subtask 1", done: false },
    { id: "c2", title: "Subtask 2", done: false },
    { id: "c3", title: "Subtask 3", done: false },
    { id: "c4", title: "Subtask 4", done: false },
  ],
  comments: [
    { id: "1", author: "Sofia Pereira", avatarColor: "#fa520f", text: "Started a draft of the spec — should be ready for review tomorrow.", createdAt: "2 days ago" },
    { id: "2", author: "Miguel Costa", avatarColor: "#5a3d0f", text: "Looks great. One concern: how does this interact with the live-reload toggle (BEAM-204)?", createdAt: "1 day ago" },
    { id: "3", author: "Sofia Pereira", avatarColor: "#fa520f", text: "Good catch. I'll add a section on that and ping you when it's ready.", createdAt: "4 hours ago" },
  ],
  attachments: [],
};

export function LiveReact() {
  const hostRef = useRef<HTMLDivElement | null>(null);
  const [modalOpen, setModalOpen] = useState(() => new URLSearchParams(window.location.search).get("modal") === "1");

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    while (host.firstChild) host.removeChild(host.firstChild);
    const range = document.createRange();
    range.selectNodeContents(host);
    const frag = range.createContextualFragment(refBody);
    host.appendChild(frag);
  }, []);

  return (
    <>
      <style>{refCss}</style>
      <style>{SCOPE_OVERRIDES}</style>
      <button
        type="button"
        onClick={() => setModalOpen((v) => !v)}
        style={{
          position: "absolute",
          right: 12,
          bottom: 12,
          zIndex: 200,
          padding: "6px 12px",
          background: "#fa520f",
          color: "#fff",
          border: "none",
          borderRadius: 4,
          fontSize: 12,
          fontWeight: 600,
          cursor: "pointer",
        }}
      >
        {modalOpen ? "close modal" : "show card modal"}
      </button>
      <div ref={hostRef} className="preview-host" />
      <KanbanCardDetail
        card={beam204}
        open={modalOpen}
        onClose={() => setModalOpen(false)}
        readOnly
      />
    </>
  );
}
