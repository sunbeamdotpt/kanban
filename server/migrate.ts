import sql from "./db.ts";

const MIGRATIONS = [
  {
    name: "001_create_projects",
    up: `
      CREATE TABLE IF NOT EXISTS projects (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name        TEXT NOT NULL,
        slug        TEXT NOT NULL UNIQUE,
        description TEXT DEFAULT '',
        owner_id    TEXT NOT NULL,
        visibility  TEXT NOT NULL DEFAULT 'private',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `,
  },
  {
    name: "002_create_project_members",
    up: `
      CREATE TABLE IF NOT EXISTS project_members (
        project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
        user_id     TEXT NOT NULL,
        role        TEXT NOT NULL DEFAULT 'viewer',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (project_id, user_id)
      );
      CREATE INDEX IF NOT EXISTS idx_project_members_user ON project_members(user_id);
    `,
  },
  {
    name: "003_create_board_templates",
    up: `
      CREATE TABLE IF NOT EXISTS board_templates (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        name        TEXT NOT NULL,
        description TEXT DEFAULT '',
        columns     JSONB NOT NULL DEFAULT '[]',
        created_by  TEXT,
        is_global   BOOLEAN NOT NULL DEFAULT false,
        project_id  UUID REFERENCES projects(id) ON DELETE CASCADE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `,
  },
  {
    name: "004_create_boards",
    up: `
      CREATE TABLE IF NOT EXISTS boards (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        project_id  UUID NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
        name        TEXT NOT NULL,
        slug        TEXT NOT NULL,
        description TEXT DEFAULT '',
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (project_id, slug)
      );
      CREATE INDEX IF NOT EXISTS idx_boards_project ON boards(project_id);
    `,
  },
  {
    name: "005_create_columns",
    up: `
      CREATE TABLE IF NOT EXISTS columns (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        board_id    UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
        title       TEXT NOT NULL,
        position    INT NOT NULL DEFAULT 0,
        color       TEXT,
        wip_limit   INT,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS idx_columns_board ON columns(board_id);
    `,
  },
  {
    name: "006_create_cards",
    up: `
      CREATE TABLE IF NOT EXISTS cards (
        id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        column_id     UUID NOT NULL REFERENCES columns(id) ON DELETE CASCADE,
        board_id      UUID NOT NULL REFERENCES boards(id) ON DELETE CASCADE,
        title         TEXT NOT NULL,
        description   TEXT DEFAULT '',
        position      INT NOT NULL DEFAULT 0,
        priority      TEXT DEFAULT 'medium',
        due_date      TIMESTAMPTZ,
        labels        JSONB NOT NULL DEFAULT '[]',
        assignees     JSONB NOT NULL DEFAULT '[]',
        forgejo_links JSONB NOT NULL DEFAULT '[]',
        created_by    TEXT NOT NULL,
        created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS idx_cards_column ON cards(column_id);
      CREATE INDEX IF NOT EXISTS idx_cards_board ON cards(board_id);
    `,
  },
  {
    name: "007_create_card_attachments",
    up: `
      CREATE TABLE IF NOT EXISTS card_attachments (
        id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
        card_id     UUID NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
        filename    TEXT NOT NULL,
        mimetype    TEXT NOT NULL DEFAULT 'application/octet-stream',
        size        BIGINT NOT NULL DEFAULT 0,
        s3_key      TEXT NOT NULL,
        uploaded_by TEXT NOT NULL,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE INDEX IF NOT EXISTS idx_attachments_card ON card_attachments(card_id);
    `,
  },
  {
    name: "008_seed_global_templates",
    up: `
      INSERT INTO board_templates (name, description, columns, is_global, created_by)
      VALUES
        ('Kanban', 'Standard kanban workflow', '[
          {"title": "Backlog", "position": 0, "color": "#94a3b8"},
          {"title": "To Do", "position": 1, "color": "#60a5fa"},
          {"title": "In Progress", "position": 2, "color": "#fbbf24"},
          {"title": "Review", "position": 3, "color": "#a78bfa"},
          {"title": "Done", "position": 4, "color": "#4ade80"}
        ]', true, null),
        ('Sprint', 'Agile sprint board', '[
          {"title": "Sprint Backlog", "position": 0, "color": "#94a3b8"},
          {"title": "In Progress", "position": 1, "color": "#fbbf24"},
          {"title": "Testing", "position": 2, "color": "#a78bfa"},
          {"title": "Done", "position": 3, "color": "#4ade80"}
        ]', true, null),
        ('Simple', 'Minimal three-column board', '[
          {"title": "To Do", "position": 0, "color": "#60a5fa"},
          {"title": "Doing", "position": 1, "color": "#fbbf24"},
          {"title": "Done", "position": 2, "color": "#4ade80"}
        ]', true, null)
      ON CONFLICT DO NOTHING;
    `,
  },
];

async function migrate() {
  await sql.unsafe(`
    CREATE TABLE IF NOT EXISTS _migrations (
      name       TEXT PRIMARY KEY,
      applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );
  `);

  for (const migration of MIGRATIONS) {
    const [existing] = await sql`
      SELECT name FROM _migrations WHERE name = ${migration.name}
    `;
    if (existing) {
      console.log(`  skip: ${migration.name}`);
      continue;
    }
    console.log(`  apply: ${migration.name}`);
    await sql.unsafe(migration.up);
    await sql`INSERT INTO _migrations (name) VALUES (${migration.name})`;
  }
  console.log("Migrations complete.");
}

if (import.meta.main) {
  await migrate();
  await sql.end();
}

export { migrate };
