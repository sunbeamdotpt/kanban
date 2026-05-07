/**
 * fixtures/kanban-api.ts
 *
 * Typed API client for direct service calls in test setup/teardown.
 *
 * Stage 6a will generate TS clients from proto under
 * apps/kanban/ui/src/api/. Until then we provide a fetch-based stub
 * that hits the gRPC-Web / ConnectRPC endpoints over JSON.
 *
 * Tests that depend on the generated client are skipped with:
 *   test.skip(!apiClient.isReady, "needs apps/kanban/ui generated client")
 */

// TODO: import from kanban-ui generated proto once Stage 6a lands:
//   import { createClient } from "@connectrpc/connect";
//   import { createConnectTransport } from "@connectrpc/connect-web";
//   import { ProjectService } from "../src/api/gen/sunbeam/kanban/v1/projects_connect";
//   import { BoardService } from "../src/api/gen/sunbeam/kanban/v1/boards_connect";
//   import { CardService } from "../src/api/gen/sunbeam/kanban/v1/cards_connect";

// ── Types (mirrored from proto, minimal surface for tests) ───────────────────

export interface ApiProject {
  id: string;
  name: string;
  prefix: string;
}

export interface ApiBoard {
  id: string;
  project_id: string;
  name: string;
}

export interface ApiCard {
  id: string;
  board_id: string;
  column_id: string;
  title: string;
  revision: number;
}

export interface ApiForgejoLinkDetail {
  id: string;
  card_id: string;
  repo_owner: string;
  repo_name: string;
  issue_or_pr_number: number;
  kind: string;
  state: string;
  url: string;
}

// ── Client class ─────────────────────────────────────────────────────────────

export class KanbanApiClient {
  /**
   * isReady is false until Stage 6a's generated Connect-ES client is present.
   * Tests that need real gRPC calls skip themselves when this is false.
   */
  readonly isReady: boolean = false;

  constructor(
    private readonly baseUrl: string,
    private readonly bearerToken: string,
  ) {}

  private async rpc<T>(
    service: string,
    method: string,
    body: unknown,
  ): Promise<T> {
    const url = `${this.baseUrl}/${service}/${method}`;
    const res = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        Authorization: `Bearer ${this.bearerToken}`,
      },
      body: JSON.stringify(body),
    });
    if (!res.ok) {
      throw new Error(`RPC ${service}/${method} failed: ${res.status} ${await res.text()}`);
    }
    return res.json() as Promise<T>;
  }

  // ── ProjectService ─────────────────────────────────────────────────────────

  async listProjects(): Promise<{ projects: ApiProject[] }> {
    return this.rpc("sunbeam.kanban.v1.ProjectService", "ListProjects", {});
  }

  async createProject(name: string, prefix: string): Promise<ApiProject> {
    return this.rpc("sunbeam.kanban.v1.ProjectService", "CreateProject", {
      name,
      prefix,
      idempotency_key: crypto.randomUUID(),
    });
  }

  async deleteProject(projectId: string): Promise<void> {
    await this.rpc("sunbeam.kanban.v1.ProjectService", "DeleteProject", {
      project_id: projectId,
    });
  }

  // ── BoardService ───────────────────────────────────────────────────────────

  async createBoard(projectId: string, name: string): Promise<ApiBoard> {
    return this.rpc("sunbeam.kanban.v1.BoardService", "CreateBoard", {
      project_id: projectId,
      name,
      idempotency_key: crypto.randomUUID(),
    });
  }

  async getBoard(boardId: string): Promise<{ board: ApiBoard; columns: unknown[] }> {
    return this.rpc("sunbeam.kanban.v1.BoardService", "GetBoard", {
      board_id: boardId,
    });
  }

  // ── CardService ────────────────────────────────────────────────────────────

  async getCard(cardId: string): Promise<ApiCard> {
    return this.rpc("sunbeam.kanban.v1.CardService", "GetCard", {
      card_id: cardId,
    });
  }

  async createCard(
    boardId: string,
    columnId: string,
    title: string,
  ): Promise<ApiCard> {
    return this.rpc("sunbeam.kanban.v1.CardService", "CreateCard", {
      board_id: boardId,
      column_id: columnId,
      title,
      idempotency_key: crypto.randomUUID(),
    });
  }

  // ── ForgejoLinkService ─────────────────────────────────────────────────────

  async listLinksByCard(
    cardId: string,
  ): Promise<{ links: ApiForgejoLinkDetail[] }> {
    return this.rpc(
      "sunbeam.kanban.v1.ForgejoLinkService",
      "ListLinksByCard",
      { card_id: cardId },
    );
  }
}

// ── Factory ──────────────────────────────────────────────────────────────────

export function createKanbanApiClient(
  bearerToken: string,
): KanbanApiClient {
  const baseUrl =
    process.env.KANBAN_E2E_BASE_URL ?? "http://localhost:47823";
  return new KanbanApiClient(baseUrl, bearerToken);
}
