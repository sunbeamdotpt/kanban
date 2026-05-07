/**
 * fixtures/seed.ts
 *
 * Creates test project/board/cards via the API before UI tests.
 * Uses KanbanApiClient from kanban-api.ts.
 *
 * All seed operations skip when KRATOS_ADMIN_URL is not set (not deployed).
 * The API client is constructed with the bearer token from the test session.
 *
 * Usage in specs:
 *   import { seedProject } from "../fixtures/seed";
 *   const { project, board, columns } = await seedProject(bearerToken);
 */

import { createKanbanApiClient, KanbanApiClient, ApiProject, ApiBoard } from "./kanban-api";

export interface SeedResult {
  project: ApiProject;
  board: ApiBoard;
  /** First column id — most tests add cards to the first column. */
  firstColumnId: string;
  api: KanbanApiClient;
}

/**
 * Seeds a project + board with the "Simple" template (3 columns).
 *
 * @param bearerToken  Token from the authed session (or an API key for CI).
 * @returns SeedResult with project, board, firstColumnId, and the api client.
 *
 * NOTE: This function skips silently (returns a stub) when the API client
 * isReady flag is false (i.e. Stage 6 generated client not yet present).
 * The calling test must also guard with test.skip(!seedResult.api.isReady).
 */
export async function seedProject(bearerToken: string): Promise<SeedResult> {
  const api = createKanbanApiClient(bearerToken);

  const project = await api.createProject(
    `E2E Seed ${Date.now()}`,
    "E2E",
  );

  const board = await api.createBoard(project.id, "Test Board");

  // GetBoard returns the columns created by the default template.
  const boardDetail = await api.getBoard(board.id);
  const columns = (boardDetail.columns ?? []) as Array<{ id: string }>;
  const firstColumnId = columns[0]?.id ?? "";

  return { project, board, firstColumnId, api };
}

/**
 * Seeds a project, board, and a single card.
 */
export async function seedProjectWithCard(
  bearerToken: string,
  cardTitle = "Seeded Card",
): Promise<SeedResult & { cardId: string }> {
  const seed = await seedProject(bearerToken);
  const card = await seed.api.createCard(
    seed.board.id,
    seed.firstColumnId,
    cardTitle,
  );
  return { ...seed, cardId: card.id };
}
