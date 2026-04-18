import { useState, useEffect } from "react";
import { css } from "styled-system/css";
import { Button, Icon, TextInput } from "@sunbeam/beam-ui";
import {
  cards as cardsApi,
  forgejo as forgejoApi,
  type ForgejoSearchResult,
} from "../api/client";
import { useBoardStore } from "../stores/board";

const PRIORITIES = [
  { value: 0, label: "None", color: "" },
  { value: 1, label: "Low", color: "#5bb8a6" },
  { value: 2, label: "Medium", color: "#4a9eff" },
  { value: 3, label: "High", color: "#f59e0b" },
  { value: 4, label: "Critical", color: "#ef4444" },
];

interface Props {
  columnId: string;
  columnTitle: string;
  onClose: () => void;
}

export default function CardCreateModal({ columnId, columnTitle, onClose }: Props) {
  const [title, setTitle] = useState("");
  const [description, setDescription] = useState("");
  const [priority, setPriority] = useState(0);
  const [dueDate, setDueDate] = useState("");
  const [labels, setLabels] = useState<{ name: string; color: string }[]>([]);
  const [newLabelName, setNewLabelName] = useState("");
  const [newLabelColor, setNewLabelColor] = useState("#60a5fa");
  const [saving, setSaving] = useState(false);

  // Attachments (queued locally, uploaded after card creation)
  const [pendingFiles, setPendingFiles] = useState<File[]>([]);

  // Forgejo linking
  const [forgejoLinks, setForgejoLinks] = useState<ForgejoSearchResult[]>([]);
  const [showForgejoSearch, setShowForgejoSearch] = useState(false);
  const [forgejoQuery, setForgejoQuery] = useState("");
  const [forgejoResults, setForgejoResults] = useState<ForgejoSearchResult[]>([]);
  const [searchingForgejo, setSearchingForgejo] = useState(false);

  const { board, loadBoard } = useBoardStore();

  // Close on Escape
  useEffect(() => {
    const handleKeyDown = (e: KeyboardEvent) => {
      if (e.key === "Escape") onClose();
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => document.removeEventListener("keydown", handleKeyDown);
  }, [onClose]);

  const handleCreate = async () => {
    if (!title.trim()) return;
    setSaving(true);
    try {
      const { card } = await cardsApi.create({
        columnId,
        title,
        description: description || undefined,
        priority: priority || undefined,
        dueDate: dueDate || undefined,
        labels: labels.length > 0 ? labels : undefined,
      });
      // If we added forgejo links, update the card immediately
      if (forgejoLinks.length > 0) {
        await cardsApi.update({
          cardId: card.id,
          forgejoLinks: forgejoLinks.map((l) => ({
            type: l.type,
            repo: l.repo,
            number: l.number,
            url: l.url,
            title: l.title,
            state: l.state,
          })),
        });
      }
      // Upload queued attachments
      for (const file of pendingFiles) {
        try {
          const { uploadUrl } = await cardsApi.createAttachment({
            cardId: card.id,
            filename: file.name,
            mimetype: file.type || "application/octet-stream",
            size: file.size,
          });
          await fetch(uploadUrl, {
            method: "PUT",
            headers: { "Content-Type": file.type || "application/octet-stream" },
            body: file,
          });
        } catch (err) {
          console.error(`Upload failed for ${file.name}:`, err);
        }
      }
      if (board) await loadBoard(board.board.id);
      onClose();
    } catch (err) {
      console.error("Create card failed:", err);
      setSaving(false);
    }
  };

  const handleAddLabel = () => {
    if (!newLabelName.trim()) return;
    setLabels([...labels, { name: newLabelName.trim(), color: newLabelColor }]);
    setNewLabelName("");
  };

  const handleRemoveLabel = (index: number) => {
    setLabels(labels.filter((_, i) => i !== index));
  };

  const handleForgejoSearch = async () => {
    if (!forgejoQuery.trim()) return;
    setSearchingForgejo(true);
    try {
      const { results } = await forgejoApi.search(forgejoQuery);
      setForgejoResults(results);
    } catch { setForgejoResults([]); }
    setSearchingForgejo(false);
  };

  const handleLinkForgejo = (result: ForgejoSearchResult) => {
    setForgejoLinks([...forgejoLinks, result]);
    setForgejoResults([]);
    setForgejoQuery("");
    setShowForgejoSearch(false);
  };

  const LABEL_COLORS = ["#60a5fa", "#4ade80", "#f59e0b", "#ef4444", "#a78bfa", "#f472b6", "#94a3b8", "#fb923c"];

  return (
    <div className={backdrop} onClick={onClose}>
      <div className={modal} onClick={(e) => e.stopPropagation()}>
        {/* Header */}
        <div className={headerRow}>
          <div className={css({ display: "flex", alignItems: "center", gap: "8px" })}>
            <Icon name="add_card" size={20} />
            <span className={headerLabel}>New card in</span>
            <span className={columnBadge}>{columnTitle}</span>
          </div>
          <button className={closeBtn} onClick={onClose}>
            <Icon name="close" size={20} />
          </button>
        </div>

        {/* Body — two columns */}
        <div className={bodyGrid}>
          {/* Left: main fields */}
          <div className={mainCol}>
            {/* Title */}
            <TextInput
              label="Title"
              value={title}
              onChange={setTitle}
              placeholder="What needs to be done?"
            />

            {/* Description */}
            <div>
              <label className={fieldLabel}>Description</label>
              <textarea
                value={description}
                onChange={(e) => setDescription(e.target.value)}
                className={textArea}
                placeholder="Add details, context, or acceptance criteria..."
                rows={5}
              />
            </div>

            {/* Forgejo links */}
            <div>
              <label className={fieldLabel}>Forgejo Links</label>
              {forgejoLinks.length > 0 && (
                <div className={css({ display: "flex", flexDirection: "column", gap: "4px", marginBottom: "8px" })}>
                  {forgejoLinks.map((link, i) => (
                    <div key={i} className={linkRow}>
                      <span className={linkTypeBadge}>{link.type === "pr" ? "PR" : "Issue"}</span>
                      <span className={css({ fontSize: "13px" })}>{link.repo}#{link.number}</span>
                      <span className={css({ fontSize: "12px", color: "text.muted", flex: 1 })}>{link.title}</span>
                      <button className={closeBtn} onClick={() => setForgejoLinks(forgejoLinks.filter((_, j) => j !== i))}>
                        <Icon name="close" size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              )}
              {showForgejoSearch ? (
                <div>
                  <div className={css({ display: "flex", gap: "6px", alignItems: "flex-end" })}>
                    <div className={css({ flex: 1 })}>
                      <TextInput label="" value={forgejoQuery} onChange={setForgejoQuery} placeholder="Search issues or PRs..." />
                    </div>
                    <Button variant="ghost" onClick={handleForgejoSearch}>
                      {searchingForgejo ? "..." : "Search"}
                    </Button>
                    <Button variant="ghost" onClick={() => { setShowForgejoSearch(false); setForgejoResults([]); }}>
                      Cancel
                    </Button>
                  </div>
                  {forgejoResults.length > 0 && (
                    <div className={css({ marginTop: "6px", display: "flex", flexDirection: "column", gap: "3px" })}>
                      {forgejoResults.map((r, i) => (
                        <div key={i} className={searchResultRow} onClick={() => handleLinkForgejo(r)}>
                          <span className={linkTypeBadge}>{r.type === "pr" ? "PR" : "Issue"}</span>
                          <span>{r.repo}#{r.number}</span>
                          <span className={css({ color: "text.muted", fontSize: "12px" })}>{r.title}</span>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              ) : (
                <button className={addFieldBtn} onClick={() => setShowForgejoSearch(true)} type="button">
                  <Icon name="link" size={14} /> Link issue or PR
                </button>
              )}
            </div>

            {/* Attachments */}
            <div>
              <label className={fieldLabel}>Attachments</label>
              {pendingFiles.length > 0 && (
                <div className={css({ display: "flex", flexDirection: "column", gap: "4px", marginBottom: "8px" })}>
                  {pendingFiles.map((file, i) => (
                    <div key={i} className={fileRow}>
                      <Icon name="attach_file" size={14} />
                      <span className={css({ fontSize: "12px", flex: 1, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" })}>{file.name}</span>
                      <span className={css({ fontSize: "11px", fontFamily: "mono", color: "text.muted", flexShrink: 0 })}>
                        {file.size < 1024 ? `${file.size}B` : `${(file.size / 1024).toFixed(1)}KB`}
                      </span>
                      <button className={closeBtn} onClick={() => setPendingFiles(pendingFiles.filter((_, j) => j !== i))}>
                        <Icon name="close" size={14} />
                      </button>
                    </div>
                  ))}
                </div>
              )}
              <label className={css({ cursor: "pointer", display: "inline-block" })}>
                <button className={addFieldBtn} type="button" onClick={() => (document.getElementById("create-card-file-input") as HTMLInputElement)?.click()}>
                  <Icon name="upload_file" size={14} /> Add files
                </button>
                <input
                  id="create-card-file-input"
                  type="file"
                  multiple
                  className={css({ display: "none" })}
                  onChange={(e) => {
                    const files = Array.from(e.target.files ?? []);
                    if (files.length) setPendingFiles([...pendingFiles, ...files]);
                    e.target.value = "";
                  }}
                />
              </label>
            </div>
          </div>

          {/* Right: sidebar fields */}
          <div className={sideCol}>
            {/* Priority */}
            <div>
              <label className={fieldLabel}>Priority</label>
              <div className={priorityCol}>
                {PRIORITIES.map((p) => (
                  <button
                    key={p.value}
                    type="button"
                    className={`${priorityBtn} ${priority === p.value ? priorityBtnActive : ""}`}
                    onClick={() => setPriority(p.value)}
                  >
                    {p.color && (
                      <span
                        className={css({ width: "8px", height: "8px", borderRadius: "50%", flexShrink: 0 })}
                        style={{ backgroundColor: p.color }}
                      />
                    )}
                    {p.label}
                  </button>
                ))}
              </div>
            </div>

            {/* Due date */}
            <div>
              <label className={fieldLabel}>Due Date</label>
              <input
                type="date"
                value={dueDate}
                onChange={(e) => setDueDate(e.target.value)}
                className={dateInput}
              />
            </div>

            {/* Labels */}
            <div>
              <label className={fieldLabel}>Labels</label>
              {labels.length > 0 && (
                <div className={css({ display: "flex", flexWrap: "wrap", gap: "4px", marginBottom: "8px" })}>
                  {labels.map((l, i) => (
                    <span key={i} className={labelPill} style={{ backgroundColor: l.color }} onClick={() => handleRemoveLabel(i)}>
                      {l.name} <span className={css({ opacity: 0.7 })}>×</span>
                    </span>
                  ))}
                </div>
              )}
              <div className={css({ display: "flex", gap: "4px", alignItems: "stretch" })}>
                <input
                  type="text"
                  value={newLabelName}
                  onChange={(e) => setNewLabelName(e.target.value)}
                  placeholder="Label name"
                  className={labelInput}
                  onKeyDown={(e) => { if (e.key === "Enter") handleAddLabel(); }}
                />
                <button className={addLabelBtn} onClick={handleAddLabel} type="button">+</button>
              </div>
              <div className={css({ display: "flex", gap: "3px", marginTop: "6px", flexWrap: "wrap" })}>
                {LABEL_COLORS.map((c) => (
                  <button
                    key={c}
                    type="button"
                    className={`${colorDot} ${newLabelColor === c ? colorDotActive : ""}`}
                    style={{ backgroundColor: c }}
                    onClick={() => setNewLabelColor(c)}
                  />
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Actions */}
        <div className={actions}>
          <Button variant="ghost" onClick={onClose}>Cancel</Button>
          <Button variant="primary" onClick={handleCreate} disabled={!title.trim() || saving}>
            {saving ? "Creating..." : "Create card"}
          </Button>
        </div>
      </div>
    </div>
  );
}

// ─── Styles ────────────────────────────────────────────────────────────────

const backdrop = css({
  position: "fixed",
  inset: 0,
  backgroundColor: "rgba(0,0,0,0.5)",
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  zIndex: 50,
  padding: "24px",
});

const modal = css({
  backgroundColor: "bg.page",
  border: "1px solid",
  borderColor: "border.default",
  shadow: "golden",
  width: "100%",
  maxWidth: "720px",
  maxHeight: "85vh",
  overflowY: "auto",
  padding: "24px",
  display: "flex",
  flexDirection: "column",
  gap: "16px",
});

const headerRow = css({
  display: "flex",
  justifyContent: "space-between",
  alignItems: "center",
});

const headerLabel = css({
  fontSize: "14px",
  fontWeight: "heading",
  fontFamily: "heading",
  color: "text.secondary",
});

const columnBadge = css({
  fontSize: "11px",
  fontWeight: "button",
  textTransform: "uppercase",
  letterSpacing: "0.1em",
  color: "text.muted",
  padding: "2px 10px",
  border: "1px solid",
  borderColor: "border.default",
});

const closeBtn = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "center",
  background: "none",
  border: "none",
  cursor: "pointer",
  color: "text.muted",
  padding: "4px",
  _hover: { color: "sunbeam.orange" },
});

const bodyGrid = css({
  display: "flex",
  gap: "24px",
  flexDirection: { base: "column", md: "row" },
});

const mainCol = css({
  flex: 1,
  minWidth: 0,
  display: "flex",
  flexDirection: "column",
  gap: "14px",
});

const sideCol = css({
  width: { base: "100%", md: "200px" },
  flexShrink: 0,
  display: "flex",
  flexDirection: "column",
  gap: "16px",
});

const fieldLabel = css({
  fontSize: "12px",
  fontWeight: "button",
  textTransform: "uppercase",
  letterSpacing: "0.05em",
  color: "text.secondary",
  display: "block",
  marginBottom: "6px",
});

const textArea = css({
  width: "100%",
  padding: "10px 12px",
  fontSize: "14px",
  border: "1px solid",
  borderColor: "border.default",
  backgroundColor: "bg.card",
  color: "text.primary",
  fontFamily: "body",
  resize: "vertical",
  outline: "none",
  transition: "all 0.15s ease",
  _focus: { ringWidth: "2px", ringColor: "sunbeam.orange", borderColor: "transparent" },
  _placeholder: { color: "text.muted" },
});

const priorityCol = css({
  display: "flex",
  flexDirection: "column",
  gap: "4px",
});

const priorityBtn = css({
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "5px 10px",
  fontSize: "12px",
  fontWeight: "button",
  border: "1px solid",
  borderColor: "border.default",
  backgroundColor: "transparent",
  color: "text.secondary",
  cursor: "pointer",
  transition: "all 0.15s ease",
  textAlign: "left",
  _hover: { borderColor: "sunbeam.orange" },
});

const priorityBtnActive = css({
  borderColor: "sunbeam.orange",
  color: "sunbeam.orange",
  backgroundColor: "bg.card",
});

const dateInput = css({
  width: "100%",
  padding: "7px 10px",
  fontSize: "13px",
  border: "1px solid",
  borderColor: "border.default",
  backgroundColor: "bg.card",
  color: "text.primary",
  fontFamily: "mono",
  outline: "none",
  _focus: { ringWidth: "2px", ringColor: "sunbeam.orange", borderColor: "transparent" },
});

const labelPill = css({
  fontSize: "11px",
  fontWeight: "button",
  color: "white",
  padding: "2px 8px",
  cursor: "pointer",
  _hover: { opacity: 0.8 },
});

const labelInput = css({
  flex: 1,
  padding: "5px 8px",
  fontSize: "12px",
  border: "1px solid",
  borderColor: "border.default",
  backgroundColor: "bg.card",
  color: "text.primary",
  outline: "none",
  _focus: { borderColor: "sunbeam.orange" },
});

const addLabelBtn = css({
  padding: "5px 10px",
  fontSize: "14px",
  fontWeight: "button",
  border: "1px solid",
  borderColor: "border.default",
  backgroundColor: "transparent",
  color: "text.muted",
  cursor: "pointer",
  _hover: { borderColor: "sunbeam.orange", color: "sunbeam.orange" },
});

const colorDot = css({
  width: "18px",
  height: "18px",
  borderRadius: "50%",
  border: "2px solid transparent",
  cursor: "pointer",
  transition: "border-color 0.15s",
  _hover: { borderColor: "text.muted" },
});

const colorDotActive = css({
  borderColor: "sunbeam.orange",
});

const linkRow = css({
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "4px 8px",
  backgroundColor: "bg.card",
  border: "1px solid",
  borderColor: "border.subtle",
  fontSize: "13px",
});

const linkTypeBadge = css({
  fontSize: "10px",
  fontFamily: "mono",
  padding: "0 4px",
  border: "1px solid",
  borderColor: "border.default",
  flexShrink: 0,
});

const searchResultRow = css({
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "6px 8px",
  border: "1px solid",
  borderColor: "border.subtle",
  cursor: "pointer",
  fontSize: "13px",
  _hover: { borderColor: "sunbeam.orange" },
});

const addFieldBtn = css({
  display: "flex",
  alignItems: "center",
  gap: "5px",
  padding: "6px 0",
  fontSize: "12px",
  color: "text.muted",
  background: "none",
  border: "none",
  cursor: "pointer",
  _hover: { color: "sunbeam.orange" },
});

const actions = css({
  display: "flex",
  gap: "8px",
  justifyContent: "flex-end",
  paddingTop: "8px",
  borderTop: "1px solid",
  borderColor: "border.subtle",
});

const fileRow = css({
  display: "flex",
  alignItems: "center",
  gap: "6px",
  padding: "4px 8px",
  backgroundColor: "bg.card",
  border: "1px solid",
  borderColor: "border.subtle",
  minWidth: 0,
});
