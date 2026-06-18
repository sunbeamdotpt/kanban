-- github_links: soft-links to GitHub issues/PRs (not cascaded on card delete)

CREATE TABLE github_links (
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

CREATE INDEX idx_github_links_card ON github_links(card_id);
CREATE INDEX idx_github_links_repo_issue ON github_links(repo, issue_id);
