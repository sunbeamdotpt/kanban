import { render, screen } from "@testing-library/react";
import { describe, it, expect, afterEach } from "vitest";
import { MemoryRouter } from "react-router";
import { authActions } from "@sunbeam/g2v/state";
import { FrameworkProvider } from "@sunbeam/g2v/providers";
import { createMockTransport } from "@sunbeam/g2v/testing";
import { App } from "./App";

const mockTransport = createMockTransport({ routes: () => {} });

describe("App", () => {
  afterEach(() => {
    authActions.logout();
  });

  it("renders the booting placeholder when authenticated", () => {
    authActions.loginSuccess({ accessToken: "tok", claims: { sub: "user:1" } });
    render(
      <FrameworkProvider transport={mockTransport}>
        <MemoryRouter initialEntries={["/"]}>
          <App />
        </MemoryRouter>
      </FrameworkProvider>,
    );
    expect(screen.getByText(/Sunbeam Kanban/i)).toBeInTheDocument();
  });
});
