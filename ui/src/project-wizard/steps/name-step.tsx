/**
 * name-step.tsx
 *
 * Step 1: Project name + prefix input.
 * Prefix auto-fills from name uppercase initials.
 */

import { useEffect } from "react";
import { css } from "styled-system/css";

interface NameStepProps {
  name: string;
  onNameChange: (name: string) => void;
  prefix: string;
  onPrefixChange: (prefix: string) => void;
}

export function NameStep({
  name,
  onNameChange,
  prefix,
  onPrefixChange,
}: NameStepProps) {
  // Auto-generate prefix from name
  useEffect(() => {
    if (!prefix && name.trim()) {
      const auto = name
        .split(/\s+/)
        .map((w) => w[0])
        .filter(Boolean)
        .join("")
        .slice(0, 6)
        .toUpperCase();
      onPrefixChange(auto);
    }
  }, [name, prefix, onPrefixChange]);

  return (
    <div className={containerStyle}>
      <div className={fieldStyle}>
        <label className={labelStyle}>Project name</label>
        <input
          type="text"
          autoFocus
          value={name}
          onChange={(e) => onNameChange(e.target.value)}
          placeholder="Acme Mobile"
          minLength={2}
          maxLength={60}
          className={inputStyle}
        />
        <span className={helperStyle}>2-60 characters</span>
      </div>

      <div className={fieldStyle}>
        <label className={labelStyle}>Card prefix</label>
        <input
          type="text"
          value={prefix}
          onChange={(e) =>
            onPrefixChange(e.target.value.toUpperCase().slice(0, 6))
          }
          placeholder="ACM"
          maxLength={6}
          className={inputMonoStyle}
        />
        <span className={helperStyle}>2-6 uppercase letters</span>
      </div>
    </div>
  );
}

const containerStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "20px",
});

const fieldStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "6px",
});

const labelStyle = css({
  fontSize: "13px",
  fontWeight: "500",
  color: "var(--beam-color-text)",
});

const inputStyle = css({
  padding: "8px 12px",
  fontSize: "14px",
  border: "1px solid var(--beam-color-border)",
  borderRadius: "4px",
  backgroundColor: "var(--beam-color-bg-primary)",
  color: "var(--beam-color-text)",
  outline: "none",
  transition: "border-color 0.2s",
  _focus: {
    borderColor: "var(--beam-color-accent)",
  },
});

const inputMonoStyle = css({
  padding: "8px 12px",
  fontSize: "14px",
  fontFamily: "monospace",
  border: "1px solid var(--beam-color-border)",
  borderRadius: "4px",
  backgroundColor: "var(--beam-color-bg-primary)",
  color: "var(--beam-color-text)",
  outline: "none",
  transition: "border-color 0.2s",
  _focus: {
    borderColor: "var(--beam-color-accent)",
  },
});

const helperStyle = css({
  fontSize: "12px",
  color: "var(--beam-color-text-tertiary)",
});
