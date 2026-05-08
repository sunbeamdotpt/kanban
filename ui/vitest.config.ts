import { resolve } from "path";
import { defineConfig, type Plugin } from "vitest/config";
import react from "@vitejs/plugin-react";

const reactPath = resolve(__dirname, "node_modules/react");
const reactDomPath = resolve(__dirname, "node_modules/react-dom");

/**
 * Forces all react/react-dom imports to resolve to the kanban app's copies,
 * regardless of which node_modules subtree the importer lives in.
 * This prevents dual-React issues with beam-ui's @ark-ui/react dependencies.
 */
function singleReactPlugin(): Plugin {
  const reactEntries: Record<string, string> = {
    react: resolve(reactPath, "index.js"),
    "react/jsx-runtime": resolve(reactPath, "jsx-runtime.js"),
    "react/jsx-dev-runtime": resolve(reactPath, "jsx-dev-runtime.js"),
    "react-dom": resolve(reactDomPath, "index.js"),
    "react-dom/client": resolve(reactDomPath, "client.js"),
    "react-dom/server": resolve(reactDomPath, "server.js"),
  };

  return {
    name: "single-react",
    enforce: "pre",
    resolveId(id) {
      if (id in reactEntries) {
        return { id: reactEntries[id], moduleSideEffects: false };
      }
      return null;
    },
  };
}

export default defineConfig({
  plugins: [singleReactPlugin(), react()],
  resolve: {
    alias: {
      "styled-system": resolve(__dirname, "styled-system"),
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
    server: {
      deps: {
        // Inline Ark UI and its Zag dependency chain so the singleReactPlugin
        // resolveId hook applies to them (external deps bypass Vite plugins).
        inline: [/@ark-ui\/react/, /@zag-js\//],
      },
    },
  },
});
