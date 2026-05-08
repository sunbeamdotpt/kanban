/**
 * review-step.tsx
 *
 * Step 3: Summary of project settings + error display.
 */

import { css } from "styled-system/css";
import { Icon } from "@sunbeam/beam-ui";

interface ReviewStepProps {
  name: string;
  prefix: string;
  icon: string;
  color: string;
  error?: Error | null;
}

export function ReviewStep({
  name,
  prefix,
  icon,
  color,
  error,
}: ReviewStepProps) {
  return (
    <div className={containerStyle}>
      {error && (
        <div className={errorBoxStyle}>
          <span className={errorIconStyle}>!</span>
          <div className={errorContentStyle}>
            <div className={errorTitleStyle}>Error creating project</div>
            <div className={errorMessageStyle}>{error.message}</div>
          </div>
        </div>
      )}

      <div className={summaryStyle}>
        <h4 className={summaryTitleStyle}>You're creating</h4>

        <div className={summaryRowStyle}>
          <div
            className={previewIconStyle}
            style={{ backgroundColor: color }}
          >
            <Icon name={icon} size={16} />
          </div>

          <div className={summaryInfoStyle}>
            <div className={projectNameStyle}>{name || "Untitled project"}</div>
            <div className={summaryMetaStyle}>
              Prefix <code>{prefix || "ABC"}</code>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}

const containerStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "16px",
});

const errorBoxStyle = css({
  display: "flex",
  gap: "12px",
  padding: "12px 16px",
  backgroundColor: "rgba(220, 38, 38, 0.1)",
  border: "1px solid rgba(220, 38, 38, 0.3)",
  borderRadius: "6px",
});

const errorIconStyle = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  width: "24px",
  height: "24px",
  borderRadius: "50%",
  backgroundColor: "#dc2626",
  color: "#fff",
  fontWeight: "bold",
  fontSize: "14px",
  flexShrink: 0,
});

const errorContentStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "4px",
});

const errorTitleStyle = css({
  fontSize: "13px",
  fontWeight: "600",
  color: "#dc2626",
});

const errorMessageStyle = css({
  fontSize: "12px",
  color: "var(--beam-color-text-secondary)",
});

const summaryStyle = css({
  padding: "16px",
  backgroundColor: "var(--beam-color-bg-secondary)",
  borderRadius: "6px",
  border: "1px solid var(--beam-color-border)",
});

const summaryTitleStyle = css({
  margin: "0 0 12px 0",
  fontSize: "12px",
  fontWeight: "600",
  color: "var(--beam-color-text-tertiary)",
  textTransform: "uppercase",
  letterSpacing: "0.5px",
});

const summaryRowStyle = css({
  display: "flex",
  gap: "12px",
  alignItems: "flex-start",
});

const previewIconStyle = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  width: "40px",
  height: "40px",
  borderRadius: "6px",
  flexShrink: 0,
});

const summaryInfoStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "4px",
  flex: 1,
});

const projectNameStyle = css({
  fontSize: "14px",
  fontWeight: "600",
  color: "var(--beam-color-text)",
});

const summaryMetaStyle = css({
  fontSize: "12px",
  color: "var(--beam-color-text-tertiary)",

  "& code": {
    fontFamily: "monospace",
    fontSize: "11px",
    padding: "2px 4px",
    backgroundColor: "var(--beam-color-bg-tertiary)",
    borderRadius: "2px",
    color: "var(--beam-color-text)",
  },
});
