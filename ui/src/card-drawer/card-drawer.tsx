/**
 * CardDrawer — opens when the URL has ?card=<cardId>.
 * Fetches card data, renders KanbanCardDetail from beam-ui, allows editing and deletion.
 */

import { useSearchParams } from "react-router";
import { useCallback, useMemo } from "react";
import { KanbanCardDetail, type KanbanCardData } from "@sunbeam/beam-ui/components/ui/kanban-card-detail";
import { useRpcMutation } from "@sunbeam/g2v";
import { CardService, type Card } from "../gen/sunbeam/kanban/v1/cards_pb";
import { useCard } from "./use-card";
import { ulid } from "ulid";

/**
 * CardDrawer component that opens when ?card=<id> is in the URL.
 * Fetches the card, displays it in KanbanCardDetail, and handles save/delete operations.
 */
export function CardDrawer() {
  const [searchParams, setSearchParams] = useSearchParams();
  const cardId = searchParams.get("card");

  // Fetch the card
  const { data: card, isLoading: cardLoading } = useCard(cardId);

  // UpdateCard mutation
  const { mutate: updateCard, isPending: updateLoading } = useRpcMutation(
    CardService,
    "updateCard",
    {
      onSuccess: () => {
        // The update succeeded; stream events will update the board state.
        // Close the drawer by clearing the query param.
        setSearchParams((prev) => {
          const next = new URLSearchParams(prev);
          next.delete("card");
          return next;
        });
      },
    }
  );

  // DeleteCard mutation
  const { mutate: deleteCard } = useRpcMutation(
    CardService,
    "deleteCard",
    {
      onSuccess: () => {
        // Card deleted; close the drawer.
        setSearchParams((prev) => {
          const next = new URLSearchParams(prev);
          next.delete("card");
          return next;
        });
      },
    }
  );

  // Handle close: clear the ?card= query param
  const handleClose = useCallback(() => {
    setSearchParams((prev) => {
      const next = new URLSearchParams(prev);
      next.delete("card");
      return next;
    });
  }, [setSearchParams]);

  // Handle save: call UpdateCard with sparse mask
  const handleSave = useCallback(
    (updatedCard: KanbanCardData) => {
      if (!card) return;

      updateCard({
        cardId: card.id,
        card: {
          id: card.id,
          projectId: card.projectId,
          boardId: card.boardId,
          columnId: card.columnId,
          ref: card.ref,
          title: updatedCard.title,
          description: updatedCard.description,
          priority: card.priority,
          due: card.due,
          completedAt: card.completedAt,
          blocked: card.blocked,
          cover: card.cover,
          milestoneId: updatedCard.milestone,
          position: card.position,
          labels: card.labels,
          assignees: card.assignees,
          checklist: card.checklist,
          forgejoLinks: card.forgejoLinks,
          commentsCount: card.commentsCount,
          attachmentsCount: card.attachmentsCount,
          revision: card.revision,
          createdAt: card.createdAt,
          updatedAt: card.updatedAt,
        },
        updateMask: {
          paths: ["title", "description"],
        },
        idempotencyKey: ulid(),
      });
    },
    [card, updateCard]
  );

  // Handle delete: call DeleteCard
  const handleDelete = useCallback(
    (id: string) => {
      // In a real app, we'd show a confirmation dialog here.
      if (confirm("Are you sure you want to delete this card?")) {
        deleteCard({ cardId: id });
      }
    },
    [deleteCard]
  );

  // Convert Card proto to KanbanCardData for beam-ui
  const cardData: KanbanCardData | undefined = useMemo(() => {
    if (!card) return undefined;

    return {
      id: card.id,
      title: card.title,
      description: card.description || undefined,
      labels: card.labels.map((l: any) => ({
        name: l.name,
        color: l.style, // style token name, not a hex code
      })),
      assignees: card.assignees.map((a: any) => ({
        name: a.displayName || a.subject,
        avatarUrl: a.avatarUrl || undefined,
      })),
      milestone: undefined, // TODO: resolve milestone title from milestoneId
      dueDate: card.due ? new Date(card.due.toDate()).toISOString() : undefined,
      status: card.completedAt ? "done" : "open",
      priority:
        card.priority === 1
          ? "low"
          : card.priority === 2
            ? "medium"
            : card.priority === 3
              ? "high"
              : card.priority === 4
                ? "critical"
                : undefined,
      createdAt: card.createdAt ? new Date(card.createdAt.toDate()).toISOString() : undefined,
      updatedAt: card.updatedAt ? new Date(card.updatedAt.toDate()).toISOString() : undefined,
    };
  }, [card]);

  if (!cardId) {
    return null; // Don't render if no card ID in URL
  }

  return (
    <KanbanCardDetail
      card={cardData || { id: "", title: "Loading..." }}
      open={!!cardId && !!cardData}
      onClose={handleClose}
      onSave={handleSave}
      onDelete={handleDelete}
      readOnly={cardLoading || updateLoading}
    />
  );
}
