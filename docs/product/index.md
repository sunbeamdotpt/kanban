---
license: AGPL-3.0-or-later
title: Kanban Product Overview
description: What Sunbeam Kanban is, who it is for, and how the pieces fit together.
category: product
order: 1
nav_order: 1
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Kanban Product Overview

Sunbeam Kanban is a real-time collaborative board-management service for creative and product teams. It lets a studio organize work into projects, boards, and cards; invite teammates; search across everything; and see changes sync live across browsers and devices.

## Who it is for

- **Project leads** creating workspaces for a show, campaign, or release.
- **Team members** tracking tasks, assets, and approvals on shared boards.
- **Integrators** linking cards to GitHub issues, design files, or internal tools.

## Core concepts

| Concept | Description |
| --- | --- |
| **Project** | A container for related boards and members. Permissions are managed at the project level. |
| **Board** | A Kanban board with columns. Boards can be private, internal, or public. |
| **Card** | A work item with title, description, priority, labels, assignees, comments, checklists, and attachments. |
| **Template** | Reusable board or card templates, either global (built-in) or project-scoped. |
| **Aggregate board** | A read-only board that rolls up cards from multiple source boards. |

## High-level capabilities

- **Real-time collaboration:** Every mutation is written to Postgres, published to NATS JetStream, and streamed to connected clients within milliseconds.
- **Fine-grained access control:** The sso-gateway (OpenFGA-backed) gates every RPC. Object IDs come from headers, not request bodies, so server-streaming calls cannot be bypassed by forging a body.
- **Global search:** OpenSearch indexes card titles, descriptions, and GitHub issue titles. Results are post-filtered through the permission backend so users only see cards they are allowed to see.
- **GitHub linking:** Cards can be linked to GitHub issues; titles, labels, and assignees are pulled on demand.
- **Attachments:** Files are uploaded directly to S3 via presigned URLs, then confirmed and stored as card attachments.

## Architecture at a glance

```
Connect-RPC clients  →  Axum/Connect-RPC server
                              │
                              ├── Postgres (projections + event_log)
                              ├── NATS JetStream (real-time fanout)
                              ├── sso-gateway (auth + permissions)
                              ├── OpenSearch (search index)
                              └── S3 (attachments)
```

For a deeper technical walkthrough, see [Architecture](../development/architecture.md). For day-to-day operations, see [Deployment](../operations/deployment.md).
