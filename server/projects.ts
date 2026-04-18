/**
 * ProjectService RPC handlers.
 */

import sql from "./db.ts";
import { RpcError, type RpcContext } from "./rpc.ts";
import {
  type Project,
  type ProjectMember,
  Visibility,
  Role,
  visibilityFromString,
  roleFromString,
  VISIBILITY_LABELS,
  ROLE_LABELS,
} from "../gen/kanban/v1/kanban_pb.ts";
import { getUserRole, checkAccess } from "./permissions.ts";

// ─── Helpers ──────────────────────────���────────────────────��───────────────

function toProject(row: Record<string, unknown>, role: Role): Project {
  return {
    id: row.id as string,
    name: row.name as string,
    slug: row.slug as string,
    description: (row.description as string) ?? "",
    ownerId: row.owner_id as string,
    visibility: visibilityFromString(row.visibility as string),
    createdAt: (row.created_at as Date).toISOString(),
    updatedAt: (row.updated_at as Date).toISOString(),
    currentUserRole: role,
  };
}

function slugify(name: string): string {
  return name
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-|-$/g, "");
}

// ─── ListProjects ──────────────────────────────────────────────────────────

export async function listProjects(
  _req: Record<string, never>,
  ctx: RpcContext,
): Promise<{ projects: Project[] }> {
  const userId = ctx.identity.id;

  // Get projects the user owns, is a member of, or that are visible
  const rows = await sql`
    SELECT DISTINCT p.* FROM projects p
    LEFT JOIN project_members pm ON pm.project_id = p.id AND pm.user_id = ${userId}
    WHERE p.owner_id = ${userId}
      OR pm.user_id IS NOT NULL
      OR p.visibility IN ('public', 'members')
    ORDER BY p.updated_at DESC
  `;

  const projects: Project[] = [];
  for (const row of rows) {
    const role = await getUserRole(row.id, userId);
    projects.push(toProject(row, role ?? Role.VIEWER));
  }

  return { projects };
}

// ─── GetProject ────────────────────────────────────────────────────────────

export async function getProject(
  req: { slug: string },
  ctx: RpcContext,
): Promise<{ project: Project }> {
  const [row] = await sql`SELECT * FROM projects WHERE slug = ${req.slug}`;
  if (!row) throw new RpcError("not_found", "Project not found");

  const role = await getUserRole(row.id, ctx.identity.id);
  if (role === null) throw new RpcError("permission_denied", "No access to this project");

  return { project: toProject(row, role) };
}

// ─── CreateProject ─────────────────────────────────────────────────────────

export async function createProject(
  req: { name: string; slug?: string; description?: string; visibility?: Visibility },
  ctx: RpcContext,
): Promise<{ project: Project }> {
  if (!req.name) throw new RpcError("invalid_argument", "Name is required");

  const slug = req.slug || slugify(req.name);
  const visibility = req.visibility ? VISIBILITY_LABELS[req.visibility] : "private";

  const [existing] = await sql`SELECT id FROM projects WHERE slug = ${slug}`;
  if (existing) throw new RpcError("already_exists", "A project with this slug already exists");

  const [row] = await sql`
    INSERT INTO projects (name, slug, description, owner_id, visibility)
    VALUES (${req.name}, ${slug}, ${req.description ?? ""}, ${ctx.identity.id}, ${visibility})
    RETURNING *
  `;

  // Add creator as owner in members table
  await sql`
    INSERT INTO project_members (project_id, user_id, role)
    VALUES (${row.id}, ${ctx.identity.id}, 'owner')
  `;

  return { project: toProject(row, Role.OWNER) };
}

// ─── UpdateProject ─────────────────────────────────────────────────────────

export async function updateProject(
  req: { slug: string; name?: string; description?: string; visibility?: Visibility },
  ctx: RpcContext,
): Promise<{ project: Project }> {
  const [project] = await sql`SELECT * FROM projects WHERE slug = ${req.slug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.ADMIN))) {
    throw new RpcError("permission_denied", "Admin access required");
  }

  const name = req.name || project.name;
  const description = req.description ?? project.description;
  const visibility = req.visibility ? VISIBILITY_LABELS[req.visibility] : project.visibility;

  const [row] = await sql`
    UPDATE projects
    SET name = ${name}, description = ${description}, visibility = ${visibility}, updated_at = now()
    WHERE id = ${project.id}
    RETURNING *
  `;

  const role = await getUserRole(row.id, ctx.identity.id);
  return { project: toProject(row, role ?? Role.OWNER) };
}

// ─── DeleteProject ─────────────────────────────────────────────────────────

export async function deleteProject(
  req: { slug: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const [project] = await sql`SELECT * FROM projects WHERE slug = ${req.slug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.OWNER))) {
    throw new RpcError("permission_denied", "Owner access required");
  }

  await sql`DELETE FROM projects WHERE id = ${project.id}`;
  return {};
}

// ─── ListMembers ────────────────────────────────��──────────────────────────

export async function listMembers(
  req: { projectSlug: string },
  ctx: RpcContext,
): Promise<{ members: ProjectMember[] }> {
  const [project] = await sql`SELECT id FROM projects WHERE slug = ${req.projectSlug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.VIEWER))) {
    throw new RpcError("permission_denied", "No access to this project");
  }

  const rows = await sql`
    SELECT * FROM project_members WHERE project_id = ${project.id} ORDER BY created_at
  `;

  return {
    members: rows.map((row) => ({
      userId: row.user_id,
      email: "",
      name: "",
      avatarUrl: "",
      role: roleFromString(row.role),
      createdAt: (row.created_at as Date).toISOString(),
    })),
  };
}

// ─── AddMember ─────────────────────────────────────────────────────────────

export async function addMember(
  req: { projectSlug: string; userId: string; role?: Role },
  ctx: RpcContext,
): Promise<{ member: ProjectMember }> {
  const [project] = await sql`SELECT id FROM projects WHERE slug = ${req.projectSlug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.ADMIN))) {
    throw new RpcError("permission_denied", "Admin access required");
  }

  if (!req.userId) throw new RpcError("invalid_argument", "User ID is required");

  const role = req.role ? ROLE_LABELS[req.role] : "viewer";

  const [row] = await sql`
    INSERT INTO project_members (project_id, user_id, role)
    VALUES (${project.id}, ${req.userId}, ${role})
    ON CONFLICT (project_id, user_id)
    DO UPDATE SET role = ${role}
    RETURNING *
  `;

  return {
    member: {
      userId: row.user_id,
      email: "",
      name: "",
      avatarUrl: "",
      role: roleFromString(row.role),
      createdAt: (row.created_at as Date).toISOString(),
    },
  };
}

// ─── UpdateMember ──────────────────────────────────────────────────────────

export async function updateMember(
  req: { projectSlug: string; userId: string; role: Role },
  ctx: RpcContext,
): Promise<{ member: ProjectMember }> {
  const [project] = await sql`SELECT id FROM projects WHERE slug = ${req.projectSlug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.ADMIN))) {
    throw new RpcError("permission_denied", "Admin access required");
  }

  const role = ROLE_LABELS[req.role] ?? "viewer";

  const [row] = await sql`
    UPDATE project_members SET role = ${role}
    WHERE project_id = ${project.id} AND user_id = ${req.userId}
    RETURNING *
  `;
  if (!row) throw new RpcError("not_found", "Member not found");

  return {
    member: {
      userId: row.user_id,
      email: "",
      name: "",
      avatarUrl: "",
      role: roleFromString(row.role),
      createdAt: (row.created_at as Date).toISOString(),
    },
  };
}

// ─── RemoveMember ──────────────────────────────────────────────────────────

export async function removeMember(
  req: { projectSlug: string; userId: string },
  ctx: RpcContext,
): Promise<Record<string, never>> {
  const [project] = await sql`SELECT id FROM projects WHERE slug = ${req.projectSlug}`;
  if (!project) throw new RpcError("not_found", "Project not found");

  if (!(await checkAccess(project.id, ctx.identity.id, Role.ADMIN))) {
    throw new RpcError("permission_denied", "Admin access required");
  }

  await sql`
    DELETE FROM project_members
    WHERE project_id = ${project.id} AND user_id = ${req.userId}
  `;
  return {};
}
