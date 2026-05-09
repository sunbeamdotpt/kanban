/**
 * KanbanShell — wraps beam-ui Shell with kanban-specific header actions
 * and the three-area KanbanLayout (sidebar + subheader + main content).
 */

import { Shell, Header, NotificationCenter, ThemeToggle } from "@sunbeam/beam-ui";
import { UserMenu } from "../chrome/user-menu";
import { KanbanLayout } from "./kanban-layout";

interface KanbanShellProps {
  children?: React.ReactNode;
}

export function KanbanShell({ children }: KanbanShellProps) {
  return (
    <Shell
      header={
        <Header
          brand="Kanban"
          fullWidth
          items={[
            <NotificationCenter
              key="notif"
              notifications={[]}
              onMarkRead={() => {}}
              onMarkAllRead={() => {}}
            />,
            <ThemeToggle key="theme" />,
            <UserMenu key="user" />,
          ]}
        />
      }
    >
      <KanbanLayout>{children}</KanbanLayout>
    </Shell>
  );
}
