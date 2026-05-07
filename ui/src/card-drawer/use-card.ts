/**
 * Hook to fetch card data from CardService.GetCard.
 * Wraps useRpcQuery with CardService and handles the cardId parameter.
 */

import { useRpcQuery, type RpcQueryOptions } from "@sunbeam/g2v";
import { CardService, type Card } from "../gen/sunbeam/kanban/v1/cards_pb";

/**
 * Fetch a card by ID from the CardService.
 *
 * @param cardId - The card ID to fetch. If falsy, the query is disabled.
 * @param options - Optional query options (staleTime, refetchInterval, enabled, etc.).
 * @returns A TanStack Query result with card data, loading state, and error.
 */
export function useCard(
  cardId: string | null | undefined,
  options?: RpcQueryOptions<Card>
) {
  return useRpcQuery(
    CardService,
    "getCard",
    { cardId: cardId || "" },
    {
      ...options,
      enabled: !!cardId && (options?.enabled !== false),
    }
  );
}
