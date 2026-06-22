---
license: AGPL-3.0-or-later
title: Templates
description: Board and card templates in Sunbeam Kanban.
category: product
order: 3
nav_order: 3
labels:
  org: sunbeam
  repo: kanban
  package: kanban
---

# Templates

Templates let teams bootstrap boards and cards from reusable defaults. There are two kinds of template: board templates and card templates.

## Visibility scopes

Both board and card templates share the same visibility model:

- **Global** — visible to every authenticated user. These are curated by the platform and are useful for common workflows.
- **Project-scoped** — only visible inside the project that owns them. They are useful for studio-specific conventions.

A template with `project_id: null` is global. A template with a non-null `project_id` is project-scoped.

## Board templates

A board template defines the shape of a new board:

- `name` and `description` — displayed when users pick a template.
- `columns` — a JSON array of column definitions (title, order, and optional WIP limit).
- `is_public` — whether boards created from this template default to public visibility.

When a user creates a board from a template, the server copies the columns into the new board. The resulting board is a normal board and can be edited independently.

## Card templates

A card template defines default values for a new card:

- `name` — template name.
- `description` — default markdown description.
- `priority` — default priority (`low`, `medium`, `high`, `urgent`).
- `color` — default card color.
- `labels` — labels to apply.
- `checklist_items` — ordered checklist items.

Card templates are used when creating a card inside a board. Like board templates, the values are copied into the new card and can be changed afterward.

## Permission model

- **List / Get** — global templates are readable by any authenticated user. Project-scoped templates require the caller to be a member of the owning project.
- **Create / Update / Delete** — requires `KanbanProject/manage` on the owning project (or platform admin privileges for global templates).

The gating is done inside the `TemplatesService` handlers after resolving the project context.

## Lifecycle

1. A project owner or platform admin creates a template.
2. Users with the appropriate visibility can list and retrieve it.
3. Users create boards or cards from the template; the template is only a starting point.
4. The owner can update or delete the template at any time. Deleting a template does not affect boards or cards created from it.
