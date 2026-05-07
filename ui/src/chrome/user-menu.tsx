/**
 * User menu: avatar dropdown with display name + email + logout.
 *
 * Reads user info from authStore.session.claims (name, email).
 * "Sign out" calls logout() from auth/oidc.ts.
 */

import { useState, useRef, useEffect } from "react";
import { useSelector } from "@legendapp/state/react";
import { authStore } from "@sunbeam/g2v/state";
import { logout, loadHydraConfig } from "../auth/oidc";
import { Avatar } from "@sunbeam/beam-ui";

/**
 * Initials badge component (fallback if avatar is not available).
 */
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
  const [isOpen, setIsOpen] = useState(false);
  const menuRef = useRef<HTMLDivElement>(null);

  // Read claims from authStore (legend-state selector).
  const claims = useSelector(() => authStore.session.get()?.claims);

  const displayName = claims?.name ?? claims?.email ?? "User";
  const email = claims?.email ?? "";
  const initials = getInitials(claims?.name, claims?.email);

  // Handle outside clicks to close dropdown.
  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (menuRef.current && !menuRef.current.contains(e.target as Node)) {
        setIsOpen(false);
      }
    }
    if (isOpen) {
      document.addEventListener("mousedown", handleClickOutside);
      return () => document.removeEventListener("mousedown", handleClickOutside);
    }
  }, [isOpen]);

  // Handle logout.
  const handleLogout = async () => {
    try {
      const cfg = loadHydraConfig();
      await logout(cfg);
    } catch (err) {
      console.error("logout failed:", err);
    }
  };

  return (
    <div ref={menuRef} style={{ position: "relative" }}>
      {/* Avatar button */}
      <button
        onClick={() => setIsOpen(!isOpen)}
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
          fontSize: "14px",
          fontWeight: "600",
          color: "var(--beam-color-text)",
        }}
        title={displayName}
      >
        {initials}
      </button>

      {/* Dropdown menu */}
      {isOpen && (
        <div
          style={{
            position: "absolute",
            top: "calc(100% + 8px)",
            right: 0,
            backgroundColor: "var(--beam-color-bg-primary)",
            border: "1px solid var(--beam-color-border)",
            borderRadius: "8px",
            boxShadow: "0 2px 12px rgba(0,0,0,0.1)",
            minWidth: "200px",
            zIndex: 1000,
          }}
        >
          {/* User info */}
          <div
            style={{
              padding: "12px 16px",
              borderBottom: "1px solid var(--beam-color-border)",
            }}
          >
            <div
              style={{
                fontSize: "13px",
                fontWeight: "600",
                color: "var(--beam-color-text)",
              }}
            >
              {displayName}
            </div>
            {email && (
              <div
                style={{
                  fontSize: "12px",
                  color: "var(--beam-color-text-tertiary)",
                  marginTop: "4px",
                }}
              >
                {email}
              </div>
            )}
          </div>

          {/* Sign out button */}
          <button
            onClick={handleLogout}
            style={{
              width: "100%",
              padding: "8px 16px",
              border: "none",
              backgroundColor: "transparent",
              color: "var(--beam-color-text)",
              cursor: "pointer",
              fontSize: "13px",
              textAlign: "left",
              transition: "background-color 0.2s",
            }}
            onMouseEnter={(e) => {
              e.currentTarget.style.backgroundColor = "var(--beam-color-bg-secondary)";
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.backgroundColor = "transparent";
            }}
          >
            Sign out
          </button>
        </div>
      )}
    </div>
  );
}
