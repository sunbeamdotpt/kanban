-- forgejo_links: soft-links to Forgejo issues/PRs (not cascaded on card delete)

CREATE TABLE forgejo_links (
  id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  card_id     UUID NOT NULL REFERENCES cards(id),
  repo        TEXT NOT NULL,
  issue_id    INT NOT NULL,
  state       TEXT NOT NULL DEFAULT 'open',
  title       TEXT,
  url         TEXT NOT NULL,
  synced_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_forgejo_links_card ON forgejo_links(card_id);
CREATE INDEX idx_forgejo_links_repo_issue ON forgejo_links(repo, issue_id);
