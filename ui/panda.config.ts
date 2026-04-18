import { defineConfig } from "@pandacss/dev";

export default defineConfig({
  preflight: true,
  include: ["./src/**/*.{ts,tsx}", "./node_modules/@sunbeam/beam-ui/src/**/*.{ts,tsx}"],
  exclude: [],
  outdir: "styled-system",

  conditions: {
    extend: {
      dark: "[data-theme=dark] &",
      light: "[data-theme=light] &",
    },
  },

  theme: {
    breakpoints: {
      sm: "640px",
      md: "768px",
      lg: "1024px",
      xl: "1280px",
    },
    extend: {
      tokens: {
        colors: {
          "sunbeam.orange": { value: "#fa520f" },
          "sunbeam.flame": { value: "#fb6424" },
          "beam.orange": { value: "#ff8105" },

          "sunshine.900": { value: "#ff8a00" },
          "sunshine.700": { value: "#ffa110" },
          "sunshine.500": { value: "#ffb83e" },
          "sunshine.300": { value: "#ffd06a" },
          "beam.gold": { value: "#ffe295" },
          "bright.yellow": { value: "#ffd900" },

          "warm.ivory": { value: "#fffaeb" },
          "cream": { value: "#fff0c2" },
          "sunbeam.black": { value: "#1f1f1f" },
          "card.dark": { value: "#2a2a2a" },
          "code.activePill": { value: "#404040" },
          "code.text": { value: "#d4d4d8" },
          "code.success": { value: "#4ade80" },

          "syn.keyword": { value: "#c084fc" },
          "syn.fn": { value: "#93c5fd" },
          "syn.string": { value: "#86efac" },
          "syn.prop": { value: "#fdba74" },
          "syn.number": { value: "#fb923c" },
          "syn.builtin": { value: "#fde047" },

          "border.warm": { value: "rgba(127, 99, 21, 0.15)" },
          "border.warmSubtle": { value: "rgba(127, 99, 21, 0.08)" },
          "border.warmDark": { value: "rgba(255, 161, 16, 0.15)" },
        },
        fonts: {
          heading: { value: "'Ysabeau Infant', Arial, ui-sans-serif, system-ui, sans-serif" },
          body: { value: "'Ysabeau Infant', Arial, ui-sans-serif, system-ui, sans-serif" },
          mono: { value: "'Monaspace Argon', 'SF Mono', 'Fira Code', monospace" },
        },
        fontWeights: {
          display: { value: "431" },
          heading: { value: "575" },
          body: { value: "647" },
          button: { value: "791" },
        },
        shadows: {
          golden: { value: "-8px 16px 39px rgba(127,99,21,0.12), -33px 64px 72px rgba(127,99,21,0.10), -73px 144px 97px rgba(127,99,21,0.06)" },
          goldenDark: { value: "-8px 16px 39px rgba(127,99,21,0.18), -33px 64px 72px rgba(127,99,21,0.14), -73px 144px 97px rgba(127,99,21,0.08)" },
          nav: { value: "0 4px 20px rgba(127,99,21,0.08)" },
          code: { value: "0 10px 30px -10px rgba(0,0,0,0.5)" },
        },
        radii: {
          sm: { value: "2px" },
          md: { value: "4px" },
          lg: { value: "12px" },
          full: { value: "9999px" },
        },
      },
      semanticTokens: {
        shadows: {
          code: {
            value: {
              base: "0 10px 30px -10px rgba(0,0,0,0.5)",
              _dark: "0 14px 40px -6px rgba(0,0,0,0.8), 0 0 0 1px rgba(255,161,16,0.08)",
            },
          },
        },
        colors: {
          "bg.page": { value: { base: "{colors.warm.ivory}", _dark: "{colors.sunbeam.black}" } },
          "bg.card": { value: { base: "{colors.cream}", _dark: "{colors.card.dark}" } },
          "bg.nav": { value: { base: "rgba(255,250,235,0.92)", _dark: "rgba(31,31,31,0.92)" } },
          "text.primary": { value: { base: "{colors.sunbeam.black}", _dark: "#ffffff" } },
          "text.secondary": { value: { base: "hsl(0,0%,24%)", _dark: "rgba(255,255,255,0.7)" } },
          "text.muted": { value: { base: "#7f6315", _dark: "rgba(255,255,255,0.4)" } },
          "border.default": { value: { base: "{colors.border.warm}", _dark: "{colors.border.warmDark}" } },
          "border.subtle": { value: { base: "{colors.border.warmSubtle}", _dark: "rgba(255,161,16,0.08)" } },
          "accent": { value: { base: "{colors.sunbeam.orange}", _dark: "{colors.sunbeam.orange}" } },
          "sectionLabel": { value: { base: "{colors.sunbeam.orange}", _dark: "{colors.sunshine.700}" } },
        },
      },
    },
  },

  globalCss: {
    body: {
      fontFamily: "body",
      fontWeight: "body",
      bg: "bg.page",
      color: "text.primary",
      lineHeight: "1.5",
      WebkitFontSmoothing: "antialiased",
    },
  },
});
