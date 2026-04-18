/**
 * TemplateService RPC handlers.
 */

import sql from "./db.ts";
import { RpcError, type RpcContext } from "./rpc.ts";
import type { BoardTemplate, TemplateColumn } from "../gen/kanban/v1/kanban_pb.ts";

function toTemplate(row: Record<string, unknown>): BoardTemplate {
  return {
    id: row.id as string,
    name: row.name as string,
    description: (row.description as string) ?? "",
    columns: (row.columns ?? []) as TemplateColumn[],
    createdBy: (row.created_by as string) ?? "",
    isGlobal: row.is_global as boolean,
    projectId: (row.project_id as string) ?? "",
    createdAt: (row.created_at as Date).toISOString(),
  };
}

export async function listTemplates(
  req: { projectId?: string },
  _ctx: RpcContext,
): Promise<{ templates: BoardTemplate[] }> {
  let rows;
  if (req.projectId) {
    rows = await sql`
      SELECT * FROM board_templates
      WHERE is_global = true OR project_id = ${req.projectId}
      ORDER BY is_global DESC, created_at
    `;
  } else {
    rows = await sql`
      SELECT * FROM board_templates
      WHERE is_global = true
      ORDER BY created_at
    `;
  }
  return { templates: rows.map(toTemplate) };
}

export async function createTemplate(
  req: {
    name: string;
    description?: string;
    columns: TemplateColumn[];
    projectId?: string;
    isGlobal?: boolean;
  },
  ctx: RpcContext,
): Promise<{ template: BoardTemplate }> {
  if (!req.name) throw new RpcError("invalid_argument", "Name is required");
  if (!req.columns?.length) throw new RpcError("invalid_argument", "At least one column is required");

  const [row] = await sql`
    INSERT INTO board_templates (name, description, columns, created_by, is_global, project_id)
    VALUES (
      ${req.name}, ${req.description ?? ""}, ${JSON.stringify(req.columns)}::jsonb,
      ${ctx.identity.id}, ${req.isGlobal ?? false}, ${req.projectId ?? null}
    )
    RETURNING *
  `;

  return { template: toTemplate(row) };
}

export async function deleteTemplate(
  req: { templateId: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const [template] = await sql`SELECT * FROM board_templates WHERE id = ${req.templateId}`;
  if (!template) throw new RpcError("not_found", "Template not found");

  // Only the creator or global templates (admin) can be deleted
  if (template.created_by && template.created_by !== ctx.identity.id) {
    throw new RpcError("permission_denied", "Only the template creator can delete it");
  }

  await sql`DELETE FROM board_templates WHERE id = ${req.templateId}`;
  return {};
}
