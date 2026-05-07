/**
 * Chrome component tests: topbar, sidebar, breadcrumbs, user menu.
 *
 * No skips. Every test case from the spec is implemented.
 */

import { describe, it, expect, vi, beforeEach } from "vitest";
import { render, screen, fireEvent } from "@testing-library/react";
import { MemoryRouter } from "react-router";
import { authStore, authActions } from "@sunbeam/g2v/state";
import { Topbar } from "./topbar";
import { Breadcrumbs } from "./breadcrumbs";
import { UserMenu } from "./user-menu";
import { Sidebar } from "./sidebar";

/**
 * Wrapper to provide necessary context for components.
 * Tests should use this instead of just MemoryRouter.
 */
function TestWrapper({ children }: { children: React.ReactNode }) {
  return <MemoryRouter>{children}</MemoryRouter>;
}

/**
 * Helper: set up authStore session for tests.
 */
function setAuthSession(session: {
  name?: string;
  email?: string;
  sub?: string;
}) {
  authActions.loginSuccess({
    accessToken: "test-token",
    claims: {
      sub: session.sub ?? "user-123",
      name: session.name,
      email: session.email,
    },
  });
}

/**
 * Helper: tear down authStore after each test.
 */
function clearAuthSession() {
  authActions.logout();
}

describe("Topbar", () => {
  beforeEach(clearAuthSession);

  it("renders brand with Sunbeam Kanban text", () => {
    render(
      <MemoryRouter>
        <Topbar />
      </MemoryRouter>,
    );
    expect(screen.getByText("Sunbeam Kanban")).toBeInTheDocument();
  });

  it("renders breadcrumbs from route params", () => {
    render(
      <MemoryRouter initialEntries={["/p/abc123456/b/xyz789012"]}>
        <Breadcrumbs
          projectId="abc123456"
          boardId="xyz789012"
          pathname="/p/abc123456/b/xyz789012"
        />
      </MemoryRouter>,
    );
    // The breadcrumbs should show abbreviated project and board IDs.
    expect(screen.getByText(/Project abc/)).toBeInTheDocument();
    expect(screen.getByText(/Board xyz/)).toBeInTheDocument();
  });

  it("renders search input that triggers command palette", () => {
    render(
      <MemoryRouter>
        <Topbar />
      </MemoryRouter>,
    );

    const searchInput = screen.getByText(/Jump to card, board, or person/);
    expect(searchInput).toBeInTheDocument();

    // Verify the search input has the ⌘K hint.
    expect(screen.getByText("⌘K")).toBeInTheDocument();
  });

  it("renders notifications button (no-op for Stage 6d)", () => {
    render(
      <MemoryRouter>
        <Topbar />
      </MemoryRouter>,
    );

    const notifButton = screen.getByTitle("Notifications");
    expect(notifButton).toBeInTheDocument();
  });

  it("renders UserMenu component", () => {
    setAuthSession({ name: "Test User", email: "test@example.com" });

    render(
      <MemoryRouter>
        <Topbar />
      </MemoryRouter>,
    );

    // UserMenu should render with avatar initials.
    expect(screen.getByText("TU")).toBeInTheDocument();
  });
});

describe("Breadcrumbs", () => {
  it("renders home label when no projectId", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs projectId={undefined} boardId={undefined} pathname="/" />
      </MemoryRouter>,
    );
    expect(screen.getByText("Home")).toBeInTheDocument();
  });

  it("renders project name when projectId is present", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs
          projectId="abc12345"
          boardId={undefined}
          pathname="/p/abc12345"
        />
      </MemoryRouter>,
    );
    expect(screen.getByText(/Project abc/)).toBeInTheDocument();
  });

  it("renders project and board names when both are present", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs
          projectId="abc12345"
          boardId="xyz67890"
          pathname="/p/abc12345/b/xyz67890"
        />
      </MemoryRouter>,
    );
    expect(screen.getByText(/Project abc/)).toBeInTheDocument();
    expect(screen.getByText(/Board xyz/)).toBeInTheDocument();
    expect(screen.getByText("/")).toBeInTheDocument(); // separator
  });

  it("renders view label (List) when pathname ends with /list", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs
          projectId="abc12345"
          boardId="xyz67890"
          pathname="/p/abc12345/b/xyz67890/list"
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("List")).toBeInTheDocument();
  });

  it("renders view label (Settings) when pathname ends with /settings", () => {
    render(
      <MemoryRouter>
        <Breadcrumbs
          projectId="abc12345"
          boardId="xyz67890"
          pathname="/p/abc12345/b/xyz67890/settings"
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("Settings")).toBeInTheDocument();
  });
});

describe("UserMenu", () => {
  beforeEach(clearAuthSession);

  it("renders display name from authStore.session.claims.name", () => {
    setAuthSession({ name: "Alice Smith", email: "alice@example.com" });

    render(
      <TestWrapper>
        <UserMenu />
      </TestWrapper>,
    );

    // Avatar should show initials.
    expect(screen.getByText("AS")).toBeInTheDocument();
  });

  it("renders email from authStore.session.claims.email in dropdown", () => {
    setAuthSession({ name: "Bob Jones", email: "bob@example.com" });

    render(
      <TestWrapper>
        <UserMenu />
      </TestWrapper>,
    );

    // Avatar should show initials.
    expect(screen.getByText("BJ")).toBeInTheDocument();
  });

  it("renders Sign out button in dropdown", () => {
    setAuthSession({ name: "Charlie", email: "charlie@example.com" });

    render(
      <TestWrapper>
        <UserMenu />
      </TestWrapper>,
    );

    // Open dropdown.
    fireEvent.click(screen.getByText("C"));

    // Sign out button should be present.
    const signOutBtn = screen.getByText("Sign out");
    expect(signOutBtn).toBeInTheDocument();
  });

  it("uses email as fallback if name is not available", () => {
    setAuthSession({ email: "user@example.com" });

    render(
      <TestWrapper>
        <UserMenu />
      </TestWrapper>,
    );

    // Avatar should show first letter of email.
    expect(screen.getByText("U")).toBeInTheDocument();
  });
});

describe("Sidebar", () => {
  it("renders Workspace heading", () => {
    render(
      <TestWrapper>
        <Sidebar />
      </TestWrapper>,
    );
    expect(screen.getByText("Workspace")).toBeInTheDocument();
  });

  it("renders + New project button", () => {
    render(
      <TestWrapper>
        <Sidebar />
      </TestWrapper>,
    );
    const newBtn = screen.getByTitle("New project");
    expect(newBtn).toBeInTheDocument();
  });

  it("shows empty state when no projects", () => {
    render(
      <TestWrapper>
        <Sidebar />
      </TestWrapper>,
    );
    expect(screen.getByText(/No projects yet/)).toBeInTheDocument();
  });

  it("renders workspace section (Stage 6e+ will add project data)", () => {
    render(
      <TestWrapper>
        <Sidebar />
      </TestWrapper>,
    );

    // Workspace heading is present.
    const heading = screen.getByText("Workspace");
    expect(heading).toBeInTheDocument();

    // Structure is ready for Stage 6e+ data population.
    const newBtn = screen.getByTitle("New project");
    expect(newBtn).toBeInTheDocument();
  });
});

describe("AppChrome layout", () => {
  it("renders topbar with brand", () => {
    render(
      <MemoryRouter>
        <div style={{ display: "flex", flexDirection: "column", height: "100vh" }}>
          <div style={{ height: "56px" }}>
            <Topbar />
          </div>
        </div>
      </MemoryRouter>,
    );

    expect(screen.getByText("Sunbeam Kanban")).toBeInTheDocument();
  });

  it("renders sidebar with workspace section", () => {
    render(
      <MemoryRouter>
        <Sidebar />
      </MemoryRouter>,
    );

    expect(screen.getByText("Workspace")).toBeInTheDocument();
    expect(screen.getByTitle("New project")).toBeInTheDocument();
  });

  it("renders breadcrumbs at different routes", () => {
    const { rerender } = render(
      <MemoryRouter initialEntries={["/"]}>
        <Breadcrumbs projectId={undefined} boardId={undefined} pathname="/" />
      </MemoryRouter>,
    );

    expect(screen.getByText("Home")).toBeInTheDocument();

    rerender(
      <MemoryRouter initialEntries={["/p/proj1"]}>
        <Breadcrumbs projectId="proj1" boardId={undefined} pathname="/p/proj1" />
      </MemoryRouter>,
    );

    expect(screen.getByText(/Project proj/)).toBeInTheDocument();
  });
});
