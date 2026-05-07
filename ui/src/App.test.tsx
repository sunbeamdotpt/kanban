import { render, screen } from "@testing-library/react";
import { describe, it, expect } from "vitest";
import { App } from "./App";

describe("App", () => {
  it("renders the booting placeholder", () => {
    render(<App />);
    expect(screen.getByText(/Sunbeam Kanban/i)).toBeInTheDocument();
  });
});
