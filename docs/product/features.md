---
license: AGPL-3.0-or-later
title: Features
description: User-facing features of Sunbeam Kanban.
category: product
order: 2
nav_order: 2
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Features

## Projects

Projects are the top-level boundary for collaboration. Each project has:

- A name, description, and owner.
- A member list synced to Keto for permission checks.
- One or more boards.
- Optional project-scoped templates.

Members can be added or removed; removal revokes both the Keto tuple and the mirrored `project_members` row.

## Boards

A board belongs to exactly one project and contains columns and cards.

- **Columns** define workflow stages. They can be reordered and have a work-in-progress limit.
- **Visibility:** boards can be `private`, `internal` (visible to all authenticated users), or `public` (visible without authentication).
- **Subscriptions:** clients can subscribe to a board to receive live card updates, column changes, and heartbeat events over a server stream.

## Cards

Cards are the unit of work. A card has:

- Metadata: title, description, priority, due date, color.
- Labels and assignees.
- A checklist with ordered items.
- Comments with edit/delete history.
- Attachments stored in S3.
- A unique project-scoped reference (e.g., `KB-42`).

Cards can be moved between columns and boards in the same project. Moving a card to a board in a different project is rejected.

## Templates

Templates speed up repeated work.

- **Board templates** define a default set of columns and optionally seeded cards.
- **Card templates** define default title, description, labels, checklist items, and priority.
- **Global templates** are visible to every authenticated user.
- **Project-scoped templates** are only visible inside their project.

See [Templates](templates.md) for the full model.

## Aggregated boards

An aggregated board is a read-only view that combines cards from multiple source boards. It is useful for cross-team dashboards or executive overviews. Source boards can be added, removed, and reordered. Visibility of individual cards is still enforced through Keto, so users only see cards they have access to.

## Real-time sync

When a user edits a card:

1. The server writes the change to Postgres and appends an `event_log` row in the same transaction.
2. The outbox dispatcher publishes the event to NATS JetStream.
3. Each pod's `BoardSubscriberRegistry` forwards the event to all connected clients subscribed to that board.
4. The client applies the event to its local TanStack Query cache.

This keeps multiple tabs, browsers, and users in sync with sub-second latency.

## Global search

Search is backed by OpenSearch.

- Query text is matched against card title, description, and linked GitHub issue title.
- Results can be filtered by project, board, label, or assignee.
- Results are paginated with opaque cursors.
- Private-board hits are dropped unless the caller has a Keto `view` relation on the board.

## GitHub integration

Cards can be linked to GitHub issues. Linking fetches the issue title, labels, and assignee. Manual resync is supported; automatic two-way sync is planned for a future release.

## Attachments

Attachments use presigned S3 URLs so the server never proxies large files:

1. The client requests a presigned upload URL.
2. The client uploads the file directly to S3.
3. The client confirms the upload, and the server records the attachment metadata.
4. Downloads use presigned GET URLs.
