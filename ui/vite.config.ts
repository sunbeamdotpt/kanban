import { resolve } from "path";
import { defineConfig } from "vite";
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
  server: {
    port: 47823,
    strictPort: true,
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
