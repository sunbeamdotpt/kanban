/**
 * AppChrome wraps the main application content with topbar + sidebar layout.
 * Uses a flex layout for responsive layout with configurable width.
 *
 * Per feedback_dont_dictate_layout_caps.md: we only enforce floors and no-overlap;
 * the user can resize freely within those bounds.
 */

import { ReactNode } from "react";
import { Outlet } from "react-router";
import { Topbar } from "./topbar";
import { Sidebar } from "./sidebar";

/**
 * Topbar height in pixels.
 */
const TOPBAR_HEIGHT = 56;

/**
 * Sidebar minimum width in pixels.
 */
const SIDEBAR_MIN_WIDTH = 240;

/**
 * Sidebar default width in pixels.
 */
const SIDEBAR_DEFAULT_WIDTH = 280;

/**
 * Main content area minimum width in pixels.
 */
const MAIN_MIN_WIDTH = 400;

export interface AppChromeProps {
  children?: ReactNode;
}

/**
 * Main application shell component.
 * Renders topbar at top, sidebar on left (resizable), main content on right.
 */
export function AppChrome({ children }: AppChromeProps) {
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        height: "100vh",
        width: "100%",
      }}
    >
      {/* Topbar: brand + breadcrumbs + search + avatar */}
      <div
        style={{
          height: `${TOPBAR_HEIGHT}px`,
          borderBottom: "1px solid var(--beam-color-border)",
          flexShrink: 0,
        }}
      >
        <Topbar />
      </div>

      {/* Main content area: sidebar + splitter + outlet */}
      <div
        style={{
          display: "flex",
          flex: 1,
          overflow: "hidden",
        }}
      >
        {/* Sidebar: workspace > project > boards tree */}
        <div
          style={{
            width: `${SIDEBAR_DEFAULT_WIDTH}px`,
            minWidth: `${SIDEBAR_MIN_WIDTH}px`,
            borderRight: "1px solid var(--beam-color-border)",
            overflow: "hidden",
            display: "flex",
            flexDirection: "column",
          }}
        >
          <Sidebar />
        </div>

        {/* Main content: outlet for route-specific pages */}
        <main
          style={{
            flex: 1,
            minWidth: `${MAIN_MIN_WIDTH}px`,
            overflow: "auto",
          }}
        >
          {children ?? <Outlet />}
        </main>
      </div>
    </div>
  );
}
