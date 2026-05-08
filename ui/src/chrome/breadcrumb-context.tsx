/**
 * Optional override for the topbar breadcrumb.
 *
 * Production routes embed projectId/boardId in the URL, and the Breadcrumbs
 * component derives crumbs via `useParams` + `useLocation`. The /__preview
 * harness mounts SettingsPage outside the production routes (URL is
 * /__preview), so useParams returns nothing and the topbar would fall back
 * to "Home". Provide this context with explicit values to force the topbar
 * to render the correct crumb without nesting another <Router>.
 */

import { createContext, useContext, type ReactNode } from "react";

export interface BreadcrumbOverrideValue {
  projectId?: string;
  boardId?: string;
  pathname?: string;
}

const BreadcrumbOverrideContext = createContext<BreadcrumbOverrideValue | null>(null);

export function BreadcrumbOverrideProvider({
  value,
  children,
}: {
  value: BreadcrumbOverrideValue;
  children: ReactNode;
}) {
  return (
    <BreadcrumbOverrideContext.Provider value={value}>
      {children}
    </BreadcrumbOverrideContext.Provider>
  );
}

export function useBreadcrumbOverride(): BreadcrumbOverrideValue | null {
  return useContext(BreadcrumbOverrideContext);
}
