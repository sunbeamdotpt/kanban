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
import { Avatar, Button, Icon, TextInput } from "@sunbeam/beam-ui";

interface ProjectInfo {
  icon: string;
  name: string;
  cardPrefix: string;
  description: string;
}

interface Member {
  id: string;
  name: string;
  email: string;
  role: string;
}

interface Board {
  id: string;
  name: string;
  description: string;
}

const initialProject: ProjectInfo = {
  icon: "palette",
  name: "Beam UI",
  cardPrefix: "BEAM",
  description: "The Sunbeam component library and design language showcase.",
};

const initialMembers: Member[] = [
  { id: "sp", name: "Sofia Pereira", email: "sp@sunbeam.pt", role: "Design lead" },
  { id: "mc", name: "Miguel Costa", email: "mc@sunbeam.pt", role: "Engineer" },
  { id: "ak", name: "Ana Kraft", email: "ak@sunbeam.pt", role: "Engineer" },
  { id: "mb", name: "Mariana Brito", email: "mb@sunbeam.pt", role: "Designer" },
  { id: "lr", name: "Luís Rocha", email: "lr@sunbeam.pt", role: "Engineer" },
];

const initialBoards: Board[] = [
  {
    id: "components",
    name: "Components",
    description: "Day-to-day work on the component catalogue — new primitives, fixes, accessibility passes.",
  },
  {
    id: "v2-migration",
    name: "v2 Migration",
    description: "Customer-facing migration tooling, breaking change docs, codemods.",
  },
];

export function SettingsPage() {
  const [project, setProject] = useState<ProjectInfo>(initialProject);
  const [members] = useState<Member[]>(initialMembers);
  const [boards] = useState<Board[]>(initialBoards);

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

      <section className={section}>
        <h3 className={sectionTitle}>
          Members <span className={sectionCount}>· {members.length}</span>
        </h3>
        <p className={sectionDesc}>People with access to all boards in this project.</p>

        <ul className={memberList}>
          {members.map((m) => (
            <li key={m.id} className={memberRow}>
              <Avatar name={m.name} size="md" />
              <div className={memberIdentity}>
                <span className={memberName}>{m.name}</span>
                <span className={memberEmail}>{m.email}</span>
              </div>
              <span className={memberRole}>{m.role}</span>
              <Button variant="ghost" onClick={() => undefined}>
                <Icon name="more_horiz" size={18} />
              </Button>
            </li>
          ))}
        </ul>

        <div className={inviteRow}>
          <Button variant="ghost" onClick={() => undefined}>
            <Icon name="person_add" size={16} /> Invite member
          </Button>
        </div>
      </section>

      <section className={section}>
        <h3 className={sectionTitle}>
          Boards <span className={sectionCount}>· {boards.length}</span>
        </h3>
        <p className={sectionDesc}>Each project can hold multiple boards. Add or rename below.</p>

        {boards.map((b, i) => (
          <div key={b.id} className={i === boards.length - 1 ? rowLast : row}>
            <div className={rowLabelGroup}>
              <p className={rowLabel}>{b.name}</p>
              <p className={rowDesc}>{b.description}</p>
            </div>
            <div className={boardActions}>
              <Button variant="ghost" onClick={() => undefined}>
                <Icon name="edit" size={14} /> Rename
              </Button>
              <Button variant="ghost" onClick={() => undefined}>
                <Icon name="delete" size={14} />
              </Button>
            </div>
          </div>
        ))}

        <div className={inviteRow}>
          <Button variant="ghost" onClick={() => undefined}>
            <Icon name="add" size={16} /> New board
          </Button>
        </div>
      </section>

      <section className={dangerSection}>
        <h3 className={dangerTitle}>Danger zone</h3>

        <div className={rowLast}>
          <div className={rowLabelGroup}>
            <p className={rowLabel}>Archive project</p>
            <p className={rowDesc}>
              Hides this project from the sidebar. Boards and cards are preserved.
            </p>
          </div>
          <Button variant="ghost" onClick={() => undefined}>
            Archive
          </Button>
        </div>
      </section>
    </div>
  );
}

const settingsRoot = css({
  padding: "48px 56px 96px",
  maxWidth: "960px",
  margin: "0 auto",
  width: "100%",
});

const eyebrow = css({
  display: "inline-flex",
  alignItems: "center",
  gap: "8px",
  fontFamily: "body",
  fontSize: "11px",
  fontWeight: "button",
  letterSpacing: "0.15em",
  textTransform: "uppercase",
  color: "sunbeam.orange",
  marginBottom: 0,
});

const titleStyle = css({
  fontFamily: "heading",
  fontSize: "3rem",
  fontWeight: "431",
  lineHeight: 1.0,
  color: "text.primary",
  margin: 0,
});

const subtitle = css({
  fontFamily: "body",
  fontSize: "15px",
  color: "text.secondary",
  marginTop: "32px",
  marginBottom: "56px",
  lineHeight: 1.5,
});

const section = css({
  borderTop: "1px solid",
  borderColor: "border.subtle",
  paddingTop: "40px",
  marginTop: "16px",
});

const sectionTitle = css({
  fontFamily: "heading",
  fontSize: "22px",
  fontWeight: "heading",
  color: "text.primary",
  margin: 0,
  marginBottom: "28px",
});

const row = css({
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: "48px",
  paddingBottom: "28px",
  marginBottom: "28px",
  borderBottom: "1px dashed",
  borderColor: "border.subtle",
});

const rowLast = css({
  display: "flex",
  alignItems: "flex-start",
  justifyContent: "space-between",
  gap: "48px",
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
  marginTop: "6px",
  lineHeight: 1.5,
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

const sectionCount = css({
  fontFamily: "body",
  fontWeight: "body",
  color: "text.muted",
});

const sectionDesc = css({
  fontFamily: "body",
  fontSize: "13px",
  color: "text.muted",
  marginTop: "-12px",
  marginBottom: "20px",
});

const memberList = css({
  listStyle: "none",
  margin: 0,
  padding: 0,
  display: "flex",
  flexDirection: "column",
  gap: "12px",
});

const memberRow = css({
  display: "flex",
  alignItems: "center",
  gap: "14px",
  padding: "10px 4px",
  borderBottom: "1px dashed",
  borderColor: "border.subtle",
  _last: { borderBottom: "none" },
});

const memberIdentity = css({
  display: "flex",
  flexDirection: "column",
  gap: "2px",
  flex: 1,
  minWidth: 0,
});

const memberName = css({
  fontFamily: "body",
  fontSize: "14px",
  fontWeight: "button",
  color: "text.primary",
});

const memberEmail = css({
  fontFamily: "mono",
  fontSize: "12px",
  color: "text.muted",
});

const memberRole = css({
  fontFamily: "body",
  fontSize: "11px",
  fontWeight: "button",
  letterSpacing: "0.12em",
  textTransform: "uppercase",
  color: "sunbeam.orange",
});

const inviteRow = css({
  marginTop: "16px",
});

const boardActions = css({
  display: "flex",
  gap: "6px",
  flexShrink: 0,
});

const dangerSection = css({
  borderTop: "1px solid",
  borderColor: "border.subtle",
  paddingTop: "28px",
  marginTop: "8px",
});

const dangerTitle = css({
  fontFamily: "heading",
  fontSize: "20px",
  fontWeight: "heading",
  color: "#dc2626",
  margin: 0,
  marginBottom: "20px",
});
