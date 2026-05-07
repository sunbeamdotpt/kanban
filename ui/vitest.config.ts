import { resolve } from "path";
import { defineConfig } from "vitest/config";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "styled-system": resolve(__dirname, "styled-system"),
      react: resolve(__dirname, "node_modules/react"),
      "react-dom": resolve(__dirname, "node_modules/react-dom"),
      "@tanstack/react-query": resolve(__dirname, "node_modules/@tanstack/react-query"),
      "@legendapp/state": resolve(__dirname, "node_modules/@legendapp/state"),
    },
    dedupe: ["react", "react-dom", "@tanstack/react-query", "@legendapp/state"],
  },
  test: {
    environment: "jsdom",
    globals: true,
    setupFiles: ["./src/test-setup.ts"],
    css: false,
    include: ["src/**/*.test.{ts,tsx}"],
    exclude: ["node_modules", "dist", "e2e", ".vite"],
  },
});
