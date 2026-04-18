import { resolve } from "path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  resolve: {
    alias: {
      "styled-system": resolve(__dirname, "styled-system"),
    },
  },
  server: {
    port: 5190,
    proxy: {
      "/api": "http://localhost:3200",
      "/kanban.v1": "http://localhost:3200",
      "/health": "http://localhost:3200",
    },
  },
  build: {
    outDir: "dist",
  },
});
