/**
 * User menu: avatar trigger with DropdownMenu showing display name + email header + logout.
 */

import { useSelector } from "@legendapp/state/react";
import { authStore } from "@sunbeam/g2v/state";
import { logout, loadHydraConfig } from "../auth/oidc";
import { Avatar, DropdownMenu } from "@sunbeam/beam-ui";

function getInitials(name?: string, email?: string): string {
  if (name) {
    return name
      .split(" ")
      .map((part) => part[0])
      .join("")
      .toUpperCase()
      .slice(0, 2);
  }
  if (email) {
    return email[0].toUpperCase();
  }
  return "?";
}

export function UserMenu() {
  const claims = useSelector(() => authStore.session.get()?.claims);

  const displayName = claims?.name ?? claims?.email ?? "User";
  const email = claims?.email ?? "";
  const initials = getInitials(claims?.name, claims?.email);

  const handleLogout = async () => {
    try {
      const cfg = loadHydraConfig();
      await logout(cfg);
    } catch (err) {
      console.error("logout failed:", err);
    }
  };

  return (
    <DropdownMenu
      positioning={{ placement: "bottom-end" }}
      groups={[
        {
          label: email ? `${displayName} · ${email}` : displayName,
          items: [
            {
              label: "Sign out",
              icon: "logout",
              onClick: handleLogout,
            },
          ],
        },
      ]}
    >
      <button
        style={{
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          width: "40px",
          height: "40px",
          borderRadius: "50%",
          border: "none",
          backgroundColor: "var(--beam-color-bg-secondary)",
          cursor: "pointer",
          padding: 0,
        }}
        title={displayName}
        type="button"
      >
        <Avatar name={displayName} size="sm" />
      </button>
    </DropdownMenu>
  );
}
