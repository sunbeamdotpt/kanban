/**
 * Topbar: brand + breadcrumbs + search + command palette + notifications + avatar.
 *
 * Renders:
 * - Brand link on left ("Sunbeam Kanban" with star icon)
 * - Breadcrumbs in center (project / board / view)
 * - Search input that triggers CommandPalette on ⌘K or click
 * - Notification bell button (no-op for Stage 6d)
 * - User menu (avatar dropdown with WhoAmI + logout)
 */

import { useCallback, useState } from "react";
import { useLocation, useParams } from "react-router";
import { CommandPalette, useCommandPaletteShortcut, NotificationCenter } from "@sunbeam/beam-ui";
import { Breadcrumbs } from "./breadcrumbs";
import { UserMenu } from "./user-menu";

/**
 * Star icon SVG component (orange accent color).
 */
function StarIcon() {
  return (
    <svg
      xmlns="http://www.w3.org/2000/svg"
      width="20"
      height="20"
      viewBox="0 0 24 24"
      fill="currentColor"
      style={{ color: "var(--beam-color-accent-orange)" }}
    >
      <path d="M12 2l3.09 6.26L22 9.27l-5 4.87 1.18 6.88L12 17.77l-6.18 3.25L7 14.14 2 9.27l6.91-1.01L12 2z" />
    </svg>
  );
}

export function Topbar() {
  const params = useParams<{ projectId?: string; boardId?: string }>();
  const location = useLocation();
  const [isPaletteOpen, setIsPaletteOpen] = useState(false);

  // Wire ⌘K shortcut to open command palette.
  useCommandPaletteShortcut(() => setIsPaletteOpen(true));

  const handleSearchClick = useCallback(() => {
    setIsPaletteOpen(true);
  }, []);

  return (
    <div
      style={{
        display: "flex",
        alignItems: "center",
        justifyContent: "space-between",
        height: "100%",
        paddingLeft: "16px",
        paddingRight: "16px",
        gap: "24px",
      }}
    >
      {/* Brand: Sunbeam Kanban */}
      <a
        href="/"
        style={{
          display: "flex",
          alignItems: "center",
          gap: "8px",
          textDecoration: "none",
          color: "var(--beam-color-text)",
          fontSize: "16px",
          fontWeight: "600",
          flexShrink: 0,
        }}
      >
        <StarIcon />
        <span>Sunbeam Kanban</span>
      </a>

      {/* Breadcrumbs: project / board / view */}
      <div style={{ flex: 1, minWidth: 0 }}>
        <Breadcrumbs
          projectId={params.projectId}
          boardId={params.boardId}
          pathname={location.pathname}
        />
      </div>

      {/* Search input + Command Palette */}
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: "8px",
          flex: 0.3,
          minWidth: "240px",
        }}
      >
        <div
          style={{
            position: "relative",
            display: "flex",
            alignItems: "center",
            flex: 1,
          }}
        >
          <span
            style={{
              position: "absolute",
              left: "10px",
              fontSize: "14px",
              pointerEvents: "none",
              color: "var(--beam-color-text-tertiary)",
            }}
          >
            🔍
          </span>
          <input
            readOnly
            onClick={handleSearchClick}
            onFocus={handleSearchClick}
            placeholder="Jump to card, board, or person…"
            aria-label="Search"
            style={{
              paddingLeft: "32px",
              paddingRight: "48px",
              paddingTop: "8px",
              paddingBottom: "8px",
              borderRadius: "6px",
              border: "1px solid var(--beam-color-border)",
              backgroundColor: "var(--beam-color-bg-secondary)",
              cursor: "pointer",
              flex: 1,
              width: "100%",
              fontSize: "14px",
              color: "var(--beam-color-text-tertiary)",
              outline: "none",
            }}
          />
          <span
            style={{
              position: "absolute",
              right: "10px",
              padding: "2px 6px",
              backgroundColor: "var(--beam-color-bg-tertiary)",
              borderRadius: "4px",
              fontSize: "12px",
              fontFamily: "monospace",
              pointerEvents: "none",
            }}
          >
            ⌘K
          </span>
        </div>
      </div>

      {/* Notifications */}
      <NotificationCenter
        notifications={[]}
        onMarkRead={() => {}}
        onMarkAllRead={() => {}}
      />

      {/* User menu (avatar + dropdown) */}
      <UserMenu />

      {/* Command Palette modal - only render if we're in a browser environment */}
      {isPaletteOpen &&
        typeof window !== "undefined" &&
        window.document && (
          <CommandPalette
            open={isPaletteOpen}
            onOpenChange={setIsPaletteOpen}
            items={[]}
            placeholder="Jump to card, board, or person…"
            emptyMessage="TODO: wire to ProjectService.ListProjects + BoardService.ListBoards in Stage 6e+"
          />
        )}
    </div>
  );
}
