/**
 * Shared selector helpers for the Kanban E2E suite.
 *
 * Convention (Stage 6 must add these testids to the FE components):
 *
 *   data-testid="card-{cardId}"            on KanbanCardView
 *   data-testid="column-{columnId}"         on KanbanColumn
 *   data-testid="card-detail-modal"         on KanbanCardDetail overlay
 *   data-testid="add-card-{columnId}"       on the "+ Add card" button per column
 *   data-testid="user-menu"                 on the avatar/user menu trigger
 *   data-testid="board-{boardId}"           on the board container
 *   data-testid="forgejo-link-{linkId}"     on each ForgejoLink badge in card detail
 *   data-testid="attachment-{attachmentId}" on each attachment row in card detail
 *   data-testid="auth-status"               text content mirrors window.__sunbeam_auth_status
 *
 * Never use class selectors (e.g. [class*="pos_fixed"]) — prefer testid + role.
 */

import type { Page, Locator } from "@playwright/test";

// ── Card selectors ───────────────────────────────────────────────────────────

export function cardLocator(page: Page, cardId: string): Locator {
  return page.locator(`[data-testid="card-${cardId}"]`);
}

export function columnLocator(page: Page, columnId: string): Locator {
  return page.locator(`[data-testid="column-${columnId}"]`);
}

export function cardDetailModal(page: Page): Locator {
  return page.locator('[data-testid="card-detail-modal"]');
}

export function addCardButton(page: Page, columnId: string): Locator {
  return page.locator(`[data-testid="add-card-${columnId}"]`);
}

export function userMenuTrigger(page: Page): Locator {
  return page.locator('[data-testid="user-menu"]');
}

export function boardContainer(page: Page, boardId: string): Locator {
  return page.locator(`[data-testid="board-${boardId}"]`);
}

export function forgejoLinkBadge(page: Page, linkId: string): Locator {
  return page.locator(`[data-testid="forgejo-link-${linkId}"]`);
}

export function attachmentRow(page: Page, attachmentId: string): Locator {
  return page.locator(`[data-testid="attachment-${attachmentId}"]`);
}

export function authStatusIndicator(page: Page): Locator {
  return page.locator('[data-testid="auth-status"]');
}

// ── Column title helpers ─────────────────────────────────────────────────────

/** Locate a column heading by its visible uppercase title. */
export function columnByTitle(page: Page, title: string): Locator {
  return page.getByRole("heading", { name: title });
}

// ── Generic board-nav helpers ────────────────────────────────────────────────

/** Returns the "+ New project" button. */
export function newProjectButton(page: Page): Locator {
  return page.getByText("+ New project");
}

/** Returns the "+ New board" button. */
export function newBoardButton(page: Page): Locator {
  return page.getByText("+ New board");
}
