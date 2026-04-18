/**
 * OpenSearch sync — indexes card data for full-text search.
 * Fire-and-forget: errors are logged but don't block the request.
 */

const OPENSEARCH_URL =
  Deno.env.get("OPENSEARCH_URL") ?? "http://localhost:9200";
const OPENSEARCH_INDEX = Deno.env.get("OPENSEARCH_INDEX") ?? "kanban-cards";
const OPENSEARCH_ENABLED = Deno.env.get("OPENSEARCH_ENABLED") === "true";

interface CardDocument {
  id: string;
  boardId: string;
  boardName: string;
  projectId: string;
  projectName: string;
  columnId: string;
  columnTitle: string;
  title: string;
  description: string;
  priority: string;
  labels: string[];
  assignees: string[];
  forgejoLinks: string[];
  createdBy: string;
  createdAt: string;
  updatedAt: string;
}

async function indexDocument(id: string, doc: CardDocument): Promise<void> {
  if (!OPENSEARCH_ENABLED) return;

  try {
    const resp = await fetch(`${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_doc/${id}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(doc),
    });
    if (!resp.ok) {
      const text = await resp.text();
      console.error(`OpenSearch index failed for card ${id}: ${resp.status} ${text}`);
    }
  } catch (err) {
    console.error(`OpenSearch index error for card ${id}:`, err);
  }
}

async function deleteDocument(id: string): Promise<void> {
  if (!OPENSEARCH_ENABLED) return;

  try {
    const resp = await fetch(`${OPENSEARCH_URL}/${OPENSEARCH_INDEX}/_doc/${id}`, {
      method: "DELETE",
    });
    if (!resp.ok && resp.status !== 404) {
      const text = await resp.text();
      console.error(`OpenSearch delete failed for card ${id}: ${resp.status} ${text}`);
    }
  } catch (err) {
    console.error(`OpenSearch delete error for card ${id}:`, err);
  }
}

/**
 * Sync a card to OpenSearch. Call after create/update.
 * Non-blocking — returns immediately.
 */
export function syncCard(card: Record<string, unknown> & {
  id: string;
  board_id: string;
  column_id: string;
  title: string;
  description: string;
  priority: string;
  labels: { name: string }[];
  assignees: { username?: string; id?: string }[];
  forgejo_links: { repo: string; number: number }[];
  created_by: string;
  created_at: string;
  updated_at: string;
}, context: {
  boardName: string;
  projectId: string;
  projectName: string;
  columnTitle: string;
}): void {
  try {
    const labels = Array.isArray(card.labels) ? card.labels : [];
    const assignees = Array.isArray(card.assignees) ? card.assignees : [];
    const forgejoLinks = Array.isArray(card.forgejo_links) ? card.forgejo_links : [];
    const doc: CardDocument = {
      id: card.id,
      boardId: card.board_id,
      boardName: context.boardName,
      projectId: context.projectId,
      projectName: context.projectName,
      columnId: card.column_id,
      columnTitle: context.columnTitle,
      title: card.title,
      description: card.description,
      priority: card.priority,
      labels: labels.map((l: { name: string }) => l.name),
      assignees: assignees.map((a: { username?: string; id?: string }) => a.username ?? a.id ?? ""),
      forgejoLinks: forgejoLinks.map((l: { repo: string; number: number }) => `${l.repo}#${l.number}`),
      createdBy: card.created_by,
      createdAt: card.created_at,
      updatedAt: card.updated_at,
    };
    // Fire-and-forget
    indexDocument(card.id, doc);
  } catch (err) {
    console.error("syncCard error:", err);
  }
}

/**
 * Remove a card from OpenSearch. Call after delete.
 * Non-blocking — returns immediately.
 */
export function removeCard(cardId: string): void {
  deleteDocument(cardId);
}

/**
 * Ensure the OpenSearch index exists with the correct mapping.
 */
export async function ensureIndex(): Promise<void> {
  if (!OPENSEARCH_ENABLED) return;

  try {
    const resp = await fetch(`${OPENSEARCH_URL}/${OPENSEARCH_INDEX}`, {
      method: "HEAD",
    });
    if (resp.status === 200) return; // Already exists

    await fetch(`${OPENSEARCH_URL}/${OPENSEARCH_INDEX}`, {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        mappings: {
          properties: {
            title: { type: "text", analyzer: "standard" },
            description: { type: "text", analyzer: "standard" },
            labels: { type: "keyword" },
            assignees: { type: "keyword" },
            forgejoLinks: { type: "keyword" },
            priority: { type: "keyword" },
            projectName: { type: "keyword" },
            boardName: { type: "keyword" },
            columnTitle: { type: "keyword" },
            createdBy: { type: "keyword" },
            createdAt: { type: "date" },
            updatedAt: { type: "date" },
          },
        },
      }),
    });
    console.log(`OpenSearch index '${OPENSEARCH_INDEX}' created.`);
  } catch (err) {
    console.error("OpenSearch ensureIndex error:", err);
  }
}
