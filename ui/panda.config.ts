import { defineConfig } from "@pandacss/dev";
import { beamPreset } from "@sunbeam/beam-ui/preset";

export default defineConfig({
  presets: [beamPreset],
  preflight: true,
  include: [
    "./src/**/*.{ts,tsx}",
    "../../../libs/beam-ui/packages/beam-ui/dist/**/*.{js,d.ts}",
  ],
  exclude: [],
  outdir: "styled-system",
});
