-- SPDX-License-Identifier: AGPL-3.0-or-later
-- Add a kind column to github_links: "issue" or "pull_request". Mirrors the
-- proto GitHubLinkDetail.kind field. Rows created before PR detection existed
-- default to 'issue'.

ALTER TABLE github_links
  ADD COLUMN kind TEXT NOT NULL DEFAULT 'issue';
