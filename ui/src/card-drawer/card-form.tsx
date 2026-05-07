/**
 * Form for editing card fields: title, description, priority, due date, milestone, assignees, labels.
 * Uses react-hook-form + zod validation from beam-ui.
 */

import { Form, FormField, z } from "@sunbeam/beam-ui/form";
import { type Card } from "../gen/sunbeam/kanban/v1/cards_pb";
import { css } from "styled-system/css";

/** Zod schema for card form validation. */
const cardFormSchema = z.object({
  title: z.string().min(1, "Title is required").max(255),
  description: z.string().optional().default(""),
  priority: z.string().optional(),
  dueDate: z.string().optional().default(""),
  milestoneId: z.string().optional().default(""),
});

type CardFormData = z.infer<typeof cardFormSchema>;

/** Props for CardForm */
interface CardFormProps {
  card: Card;
  onSubmit: (data: CardFormData) => void | Promise<void>;
  isLoading?: boolean;
}

/**
 * Form component for editing card title, description, and metadata.
 */
export function CardForm({ card, onSubmit, isLoading = false }: CardFormProps) {
  const defaultValues: CardFormData = {
    title: card.title,
    description: card.description ?? "",
    priority: "",
    dueDate: "",
    milestoneId: card.milestoneId ?? "",
  };

  return (
    <Form
      schema={cardFormSchema}
      defaultValues={defaultValues}
      onSubmit={onSubmit}
      className={formStyle}
    >
      {(methods) => (
        <>
          <div className={fieldGroup}>
            <FormField
              name="title"
              label="Title"
              methods={methods}
              className={fieldWrapper}
            />
          </div>

          <div className={fieldGroup}>
            <label className={labelStyle} htmlFor="description">
              Description
            </label>
            <textarea
              id="description"
              {...methods.register("description")}
              placeholder="Add a description..."
              className={textareaStyle}
            />
          </div>

          <div className={fieldGroup}>
            <FormField
              name="priority"
              label="Priority"
              methods={methods}
              className={fieldWrapper}
            />
          </div>

          <div className={fieldGroup}>
            <FormField
              name="dueDate"
              label="Due Date"
              methods={methods}
              className={fieldWrapper}
            />
          </div>

          <div className={fieldGroup}>
            <FormField
              name="milestoneId"
              label="Milestone"
              methods={methods}
              className={fieldWrapper}
            />
          </div>

          <div className={formActions}>
            <button
              type="submit"
              disabled={isLoading}
              className={submitButton}
            >
              {isLoading ? "Saving..." : "Save Changes"}
            </button>
          </div>
        </>
      )}
    </Form>
  );
}

/* ================================================================ */
/* Styles                                                            */
/* ================================================================ */

const formStyle = css({
  display: "flex",
  flexDirection: "column",
  gap: "16px",
});

const fieldGroup = css({
  display: "flex",
  flexDirection: "column",
  gap: "8px",
});

const fieldWrapper = css({
  display: "flex",
  flexDirection: "column",
  gap: "6px",
  marginBottom: 0,
});

const labelStyle = css({
  fontSize: "13px",
  fontWeight: "button",
  color: "text.primary",
  textTransform: "uppercase",
  letterSpacing: "0.05em",
});

const textareaStyle = css({
  padding: "10px 12px",
  fontSize: "14px",
  fontFamily: "body",
  color: "text.primary",
  backgroundColor: "bg.card",
  border: "1px solid",
  borderColor: "border.default",
  borderRadius: "0",
  outline: "none",
  minHeight: "120px",
  resize: "vertical",
  transition: "border-color 0.15s ease",
  _focus: {
    borderColor: "sunbeam.orange",
  },
});

const formActions = css({
  display: "flex",
  gap: "8px",
  paddingTop: "8px",
  borderTop: "1px solid",
  borderColor: "border.subtle",
});

const submitButton = css({
  padding: "10px 16px",
  fontSize: "14px",
  fontWeight: "button",
  color: "white",
  backgroundColor: "sunbeam.orange",
  border: "none",
  borderRadius: "0",
  cursor: "pointer",
  transition: "opacity 0.15s ease",
  _hover: {
    opacity: 0.9,
  },
  _disabled: {
    opacity: 0.5,
    cursor: "not-allowed",
  },
});
