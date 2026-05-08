/**
 * appearance-step.tsx
 *
 * Step 2: Icon selection + color picker from beam-ui semantic palette.
 */

import { css } from "styled-system/css";
import { Icon } from "@sunbeam/beam-ui";

const PROJECT_ICONS = [
  "palette",
  "rocket_launch",
  "auto_awesome",
  "flag",
  "public",
  "edit_note",
  "science",
  "campaign",
  "shopping_bag",
  "movie",
  "school",
  "devices",
  "precision_manufacturing",
  "analytics",
  "forum",
  "stadium",
];

// Beam UI semantic colors from preset
const PROJECT_COLORS = [
  { name: "Sunbeam", value: "#fa520f" },
  { name: "Sunshine", value: "#ffa110" },
  { name: "Ink", value: "#1f1f1f" },
];

interface AppearanceStepProps {
  icon: string;
  onIconChange: (icon: string) => void;
  color: string;
  onColorChange: (color: string) => void;
}

export function AppearanceStep({
  icon,
  onIconChange,
  color,
  onColorChange,
}: AppearanceStepProps) {
  return (
    <div className={containerStyle}>
      <div className={fieldStyle}>
        <label className={labelStyle}>Icon</label>
        <div className={iconGridStyle}>
          {PROJECT_ICONS.map((ic) => (
            <button
              key={ic}
              className={`${iconCellStyle} ${
                icon === ic ? iconCellActiveStyle : ""
              }`}
              onClick={() => onIconChange(ic)}
              title={ic}
              style={
                icon === ic
                  ? { backgroundColor: color, color: "#fff" }
                  : undefined
              }
            >
              <Icon name={ic} size={20} />
            </button>
          ))}
        </div>
      </div>

      <div className={fieldStyle}>
        <label className={labelStyle}>Color</label>
        <div className={colorRowStyle}>
          {PROJECT_COLORS.map((c) => (
            <button
              key={c.value}
              className={`${colorButtonStyle} ${
                color === c.value ? colorButtonActiveStyle : ""
              }`}
              style={{ backgroundColor: c.value }}
              onClick={() => onColorChange(c.value)}
              title={c.name}
            >
              {color === c.value && (
                <span style={{ color: "#fff", fontSize: "16px" }}>✓</span>
              )}
            </button>
          ))}
        </div>
      </div>
    </div>
  );
}

const containerStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "24px",
});

const fieldStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "12px",
});

const labelStyle = css({
  fontSize: "13px",
  fontWeight: "500",
  color: "var(--beam-color-text)",
});

const iconGridStyle = css({
  display: "grid",
  gridTemplateColumns: "repeat(4, 1fr)",
  gap: "8px",
});

const iconCellStyle = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  width: "100%",
  aspectRatio: "1",
  padding: "8px",
  border: "1px solid var(--beam-color-border)",
  borderRadius: "6px",
  backgroundColor: "var(--beam-color-bg-secondary)",
  cursor: "pointer",
  color: "var(--beam-color-text)",
  transition: "all 0.2s",
  _hover: {
    borderColor: "var(--beam-color-accent)",
  },
});

const iconCellActiveStyle = css({
  borderColor: "var(--beam-color-accent)",
});

const colorRowStyle = css({
  display: "flex",
  gap: "12px",
  flexWrap: "wrap",
});

const colorButtonStyle = css({
  width: "48px",
  height: "48px",
  borderRadius: "6px",
  border: "2px solid transparent",
  cursor: "pointer",
  transition: "all 0.2s",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  _hover: {
    transform: "scale(1.05)",
  },
});

const colorButtonActiveStyle = css({
  borderColor: "var(--beam-color-text)",
  boxShadow: "0 0 0 2px var(--beam-color-bg-primary)",
});
