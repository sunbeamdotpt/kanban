import { Outlet, useNavigate, useLocation } from "react-router-dom";
import { css } from "styled-system/css";
import { Icon, Button } from "@sunbeam/beam-ui";
import { useEffect, useState } from "react";
import { auth, type User } from "../api/client";

const shell = css({
  display: "flex",
  flexDirection: "column",
  height: "100vh",
});

const topBar = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  padding: "0 24px",
  height: "52px",
  borderBottom: "1px solid",
  borderColor: "border.default",
  backgroundColor: "bg.nav",
  backdropFilter: "blur(12px)",
  flexShrink: 0,
});

const brand = css({
  display: "flex",
  alignItems: "center",
  gap: "10px",
  cursor: "pointer",
});

const brandName = css({
  fontSize: "15px",
  fontWeight: "heading",
  fontFamily: "heading",
  color: "text.primary",
  letterSpacing: "-0.01em",
});

const brandAccent = css({
  color: "sunbeam.orange",
});

const navLinks = css({
  display: "flex",
  gap: "4px",
  alignItems: "center",
});

const navLink = css({
  fontSize: "13px",
  fontWeight: "button",
  padding: "6px 12px",
  color: "text.muted",
  cursor: "pointer",
  transition: "color 0.15s",
  background: "none",
  border: "none",
  _hover: { color: "sunbeam.orange" },
});

const navLinkActive = css({
  color: "sunbeam.orange",
});

const userArea = css({
  display: "flex",
  alignItems: "center",
  gap: "8px",
  fontSize: "13px",
  color: "text.secondary",
});

const main = css({
  flex: 1,
  overflow: "auto",
});

export default function AppLayout() {
  const navigate = useNavigate();
  const location = useLocation();
  const [user, setUser] = useState<User | null>(null);

  useEffect(() => {
    auth.getSession().then((s) => setUser(s.user)).catch(() => {});
  }, []);

  const isActive = (path: string) => location.pathname.startsWith(path);

  return (
    <div className={shell}>
      <header className={topBar}>
        <div className={brand} onClick={() => navigate("/")}>
          <span className={brandName}>
            <span className={brandAccent}>sunbeam</span> kanban
          </span>
        </div>

        <nav className={navLinks}>
          <button
            className={`${navLink} ${isActive("/p") || location.pathname === "/" ? navLinkActive : ""}`}
            onClick={() => navigate("/")}
          >
            Projects
          </button>
        </nav>

        <div className={userArea}>
          {user && <span>{user.name || user.email}</span>}
        </div>
      </header>

      <main className={main}>
        <Outlet />
      </main>
    </div>
  );
}
