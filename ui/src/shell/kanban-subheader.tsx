/**
 * KanbanSubheader — renders page-injected content (title, breadcrumbs, toolbar).
 */

import { css } from "styled-system/css";
import { useSubheader } from "./subheader-context";

const subheader = css({
  padding: "12px 24px",
  backgroundColor: "bg.page",
  display: "flex",
  alignItems: "center",
  gap: "16px",
  minHeight: "48px",
});

export function KanbanSubheader() {
  const { content } = useSubheader();
  return <div className={subheader}>{content}</div>;
}
