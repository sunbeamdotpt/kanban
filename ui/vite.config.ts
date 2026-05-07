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
    port: 47823,
    strictPort: true,
  },
  build: {
    outDir: "dist",
    sourcemap: true,
  },
});
