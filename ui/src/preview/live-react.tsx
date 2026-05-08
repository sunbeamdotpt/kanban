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
import { Button } from "@sunbeam/beam-ui";
import { KanbanCardDetail, type KanbanCardData } from "@sunbeam/beam-ui/components/ui/kanban-card-detail";
import { AppChrome } from "../chrome";
import { BreadcrumbOverrideProvider } from "../chrome/breadcrumb-context";
import { SettingsPage } from "../routes/settings-page";
import refCss from "./ref-styles.css?raw";
import refBody from "./ref-body.html?raw";

// Why scope these overrides:
// - The ref CSS uses `height: 100vh` on .app and assumes the page IS the kanban,
//   but here LiveReact lives in a flex pane that's `100vh - toolbar`.
// - The ref body markup wraps .app in <div id="root">, so a direct-child
//   selector wouldn't match — use display:contents on #root and a descendant
//   selector with !important to force .app to fill its actual host.
// - Cols stay at the ref's 296px min-width so cards never get smaller than
//   the design says they should. If 5 cols + sidebar exceed viewport, the
//   .boardwrap scrolls horizontally inside (its overflow:auto handles it).
const SCOPE_OVERRIDES = `
.preview-host { position: absolute; inset: 0; width: 100%; height: 100%; min-width: 0; min-height: 0; overflow: hidden; }
.preview-host #root { width: 100%; height: 100%; display: contents; }
.preview-host .app { width: 100% !important; height: 100% !important; max-width: 100%; }
.preview-host .main { min-width: 0; min-height: 0; overflow: hidden; }
.preview-host .col { max-height: 100% !important; }
.preview-host .col.col--add { display: none; }
/* The ref CSS injects unlayered h1..h5 base rules with line-height 0.95 and
   font-size 3-3.5rem, which beat Panda's layered atoms. Reapply the shape
   Panda would have used so settings/modal headings inside .preview-host
   render the way the React components specified. */
.preview-host h1 { font-family: var(--font-heading); font-weight: 431; font-size: 3rem; line-height: 1; margin: 0; color: var(--text-primary); }
.preview-host h2 { font-family: var(--font-heading); font-weight: 575; font-size: 1.5rem; line-height: 1.2; margin: 0; color: var(--text-primary); }
.preview-host h3 { font-family: var(--font-heading); font-weight: var(--fw-heading, 575); font-size: 1.375rem; line-height: 1.3; margin: 0; color: var(--text-primary); }
.preview-host h4 { font-family: var(--font-heading); font-weight: var(--fw-heading, 575); font-size: 1.125rem; line-height: 1.3; margin: 0; color: var(--text-primary); }
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
  const [showSettings, setShowSettings] = useState(() => new URLSearchParams(window.location.search).get("settings") === "1");

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    while (host.firstChild) host.removeChild(host.firstChild);
    if (showSettings) return;
    const range = document.createRange();
    range.selectNodeContents(host);
    const frag = range.createContextualFragment(refBody);
    host.appendChild(frag);
  }, [showSettings]);

  return (
    <>
      <style>{refCss}</style>
      <style>{SCOPE_OVERRIDES}</style>
      <div style={{ position: "absolute", right: 12, bottom: 12, zIndex: 200, display: "flex", gap: 6 }}>
        <Button variant="primary" onClick={() => setShowSettings((v) => !v)}>
          {showSettings ? "show board" : "show settings"}
        </Button>
        <Button variant="primary" onClick={() => setModalOpen((v) => !v)}>
          {modalOpen ? "close modal" : "show card modal"}
        </Button>
      </div>
      {showSettings ? (
        <div className="preview-host">
          <BreadcrumbOverrideProvider
            value={{
              projectId: "beam-ui",
              boardId: "components",
              pathname: "/p/beam-ui/b/components/settings",
            }}
          >
            <AppChrome>
              <SettingsPage />
            </AppChrome>
          </BreadcrumbOverrideProvider>
        </div>
      ) : (
        <div ref={hostRef} className="preview-host" />
      )}
      <KanbanCardDetail
        card={beam204}
        open={modalOpen}
        onClose={() => setModalOpen(false)}
      />
    </>
  );
}

