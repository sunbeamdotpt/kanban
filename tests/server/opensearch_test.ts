import { assertEquals } from "https://deno.land/std@0.220.0/assert/mod.ts";

// OpenSearch is disabled by default (OPENSEARCH_ENABLED !== "true")
// These tests verify the sync/remove functions don't throw when disabled.

Deno.test("syncCard — no-op when OPENSEARCH_ENABLED is not set", async () => {
  const { syncCard } = await import("../../server/opensearch.ts");

  // Should not throw
  syncCard(
    {
      id: "test-card-1",
      board_id: "board-1",
      column_id: "col-1",
      title: "Test Card",
      description: "A test card",
      priority: "medium",
      labels: [{ name: "bug" }],
      assignees: [{ username: "alice" }],
      forgejo_links: [{ repo: "org/repo", number: 42 }],
      created_by: "user-1",
      created_at: "2024-01-01T00:00:00Z",
      updated_at: "2024-01-01T00:00:00Z",
    },
    {
      boardName: "Test Board",
      projectId: "proj-1",
      projectName: "Test Project",
      columnTitle: "In Progress",
    },
  );
  // No assertion needed — just verifying no crash
});

Deno.test("removeCard — no-op when OPENSEARCH_ENABLED is not set", async () => {
  const { removeCard } = await import("../../server/opensearch.ts");
  removeCard("test-card-1");
  // No assertion needed
});

Deno.test("ensureIndex — no-op when OPENSEARCH_ENABLED is not set", async () => {
  const { ensureIndex } = await import("../../server/opensearch.ts");
  await ensureIndex();
  // No assertion needed
});
