/**
 * SettingsPage — project + board settings reached from the sidebar gear icon.
 *
 * Layout (matches Sunbeam Kanban reference):
 *   eyebrow: project icon + name
 *   h1: Project Settings
 *   subtitle: project description
 *   Project section: name / card prefix / description rows
 *
 * For now the data is hardcoded fixtures matching the reference Beam UI project.
 * TODO(stage 6h+): wire to ProjectService.GetProject + UpdateProject.
 *
 * beam-ui inventory:
 *   Icon, TextInput — used.
 *   Textarea — does not exist in beam-ui yet; the description textarea is
 *     hand-rolled with the same panda tokens as TextInput. Candidate for an
 *     upstream `Textarea` primitive (open follow-up).
 */

import { useState } from "react";
import { css } from "styled-system/css";
import { Icon, TextInput } from "@sunbeam/beam-ui";

interface ProjectInfo {
  icon: string;
  name: string;
  cardPrefix: string;
  description: string;
}

const initialProject: ProjectInfo = {
  icon: "palette",
  name: "Beam UI",
  cardPrefix: "BEAM",
  description: "The Sunbeam component library and design language showcase.",
};

export function SettingsPage() {
  const [project, setProject] = useState<ProjectInfo>(initialProject);

  return (
    <div className={settingsRoot}>
      <div className={eyebrow}>
        <Icon name={project.icon} size={14} />
        {project.name}
      </div>
      <h1 className={titleStyle}>Project Settings</h1>
      <p className={subtitle}>{project.description}</p>

      <section className={section}>
        <h3 className={sectionTitle}>Project</h3>

        <div className={row}>
          <div className={rowLabelGroup}>
            <p className={rowLabel}>Name</p>
            <p className={rowDesc}>Shown in the sidebar and breadcrumbs.</p>
          </div>
          <div className={inputCell}>
            <TextInput
              value={project.name}
              onChange={(name) => setProject({ ...project, name })}
            />
          </div>
        </div>

        <div className={row}>
          <div className={rowLabelGroup}>
            <p className={rowLabel}>Card prefix</p>
            <p className={rowDesc}>
              Auto-generated card IDs use this prefix — e.g.{" "}
              <code className={codePill}>{project.cardPrefix}-204</code>.
            </p>
          </div>
          <div className={inputCellNarrow}>
            <TextInput
              value={project.cardPrefix}
              onChange={(cardPrefix) => setProject({ ...project, cardPrefix })}
              className={monoInputOverride}
            />
          </div>
        </div>

        <div className={rowLast}>
          <div className={rowLabelGroup}>
            <p className={rowLabel}>Description</p>
            <p className={rowDesc}>Shown on the project landing card.</p>
          </div>
          <div className={inputCellWide}>
            <textarea
              className={textareaStyle}
              value={project.description}
              onChange={(e) => setProject({ ...project, description: e.target.value })}
            />
          </div>
        </div>
      </section>
    </div>
  );
}

const settingsRoot = css({
  padding: "32px 48px 64px",
  maxWidth: "920px",
  margin: "0 auto",
  width: "100%",
});

const eyebrow = css({
  display: "inline-flex",
  alignItems: "center",
  gap: "6px",
  fontFamily: "body",
  fontSize: "11px",
  fontWeight: "button",
  letterSpacing: "0.15em",
  textTransform: "uppercase",
  color: "sunbeam.orange",
  marginBottom: "6px",
});

const titleStyle = css({
  fontFamily: "heading",
  fontSize: "3rem",
  fontWeight: "431",
  lineHeight: 0.95,
  color: "text.primary",
  margin: 0,
});

const subtitle = css({
  fontFamily: "body",
  fontSize: "14px",
  color: "text.secondary",
  marginTop: "8px",
  marginBottom: "32px",
});

const section = css({
  borderTop: "1px solid",
  borderColor: "border.subtle",
  paddingTop: "28px",
  marginTop: "8px",
});

const sectionTitle = css({
  fontFamily: "heading",
  fontSize: "20px",
  fontWeight: "heading",
  color: "text.primary",
  margin: 0,
  marginBottom: "20px",
});

const row = css({
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: "32px",
  paddingBottom: "20px",
  marginBottom: "20px",
  borderBottom: "1px dashed",
  borderColor: "border.subtle",
});

const rowLast = css({
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: "32px",
});

const rowLabelGroup = css({
  flex: 1,
  minWidth: 0,
});

const rowLabel = css({
  fontFamily: "body",
  fontSize: "14px",
  fontWeight: "button",
  color: "text.primary",
  margin: 0,
});

const rowDesc = css({
  fontFamily: "body",
  fontSize: "13px",
  color: "text.muted",
  marginTop: "4px",
});

const inputCell = css({ width: "240px", flexShrink: 0 });
const inputCellNarrow = css({ width: "120px", flexShrink: 0 });
const inputCellWide = css({ width: "320px", flexShrink: 0 });

const monoInputOverride = css({
  fontFamily: "mono !important",
  letterSpacing: "0.04em",
});

const textareaStyle = css({
  width: "100%",
  minHeight: "60px",
  fontFamily: "body",
  fontSize: "14px",
  padding: "8px 10px",
  background: "transparent",
  border: "1px solid",
  borderColor: "sunbeam.orange",
  borderRadius: "sm",
  color: "text.primary",
  outline: "none",
  resize: "vertical",
  lineHeight: 1.4,
  _focus: { boxShadow: "0 0 0 2px rgba(250, 82, 15, 0.18)" },
});

const codePill = css({
  fontFamily: "mono",
  fontSize: "12px",
  padding: "1px 6px",
  background: "bg.card",
  border: "1px solid",
  borderColor: "border.subtle",
  borderRadius: "sm",
  color: "sunbeam.orange",
});
