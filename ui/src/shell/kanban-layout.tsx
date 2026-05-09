/**
 * KanbanLayout — three-area resizable layout.
 *
 *   Sidebar (full height)
 *   └─ Subheader ────────
 *      Main content
 *
 * Sidebar ↔ rest separated by a vertical draggable divider.
 * Subheader ↔ main content separated by a horizontal draggable divider.
 * All panels scroll in both directions when content overflows.
 */

import { Splitter, ScrollArea } from "@sunbeam/beam-ui";
import { css } from "styled-system/css";
import { Sidebar } from "../chrome/sidebar";
import { KanbanSubheader } from "./kanban-subheader";
import { SubheaderProvider } from "./subheader-context";

const layoutRoot = css({
  display: "flex",
  position: "absolute",
  inset: 0,
  overflow: "hidden",
});

/** Makes ScrollArea fill its parent panel */
const scrollFill = css({
  height: "100%",
  width: "100%",
});

interface KanbanLayoutProps {
  children?: React.ReactNode;
}

export function KanbanLayout({ children }: KanbanLayoutProps) {
  return (
    <SubheaderProvider>
      <div className={layoutRoot}>
        <Splitter direction="horizontal" defaultSize={21}>
          {/* Left sidebar — full height, scrolls both ways */}
          <ScrollArea direction="both" scrollbar="hover" className={scrollFill}>
            <Sidebar />
          </ScrollArea>

          {/* Right area — subheader + main content */}
          <Splitter direction="vertical" defaultSize={12}>
            <ScrollArea direction="both" scrollbar="hover" className={scrollFill}>
              <KanbanSubheader />
            </ScrollArea>
            <ScrollArea direction="both" scrollbar="hover" className={scrollFill}>
              {children}
            </ScrollArea>
          </Splitter>
        </Splitter>
      </div>
    </SubheaderProvider>
  );
}
