/**
 * project-wizard.tsx
 *
 * Multi-step modal for creating a new Kanban project.
 * Step 1: Name + prefix
 * Step 2: Icon + color
 * Step 3: Review + create
 */

import { useState } from "react";
import { useNavigate } from "react-router";
import { css } from "styled-system/css";
import { Dialog, Button } from "@sunbeam/beam-ui";
import { ulid } from "ulid";
import { NameStep } from "./steps/name-step";
import { AppearanceStep } from "./steps/appearance-step";
import { ReviewStep } from "./steps/review-step";
import { useCreateProject } from "./use-create-project";

interface ProjectWizardProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
}

export function ProjectWizard({ open, onOpenChange }: ProjectWizardProps) {
  const navigate = useNavigate();
  const createProjectMutation = useCreateProject();

  const [step, setStep] = useState(1);
  const [name, setName] = useState("");
  const [prefix, setPrefix] = useState("");
  const [icon, setIcon] = useState("palette");
  const [color, setColor] = useState("#fa520f");
  const [isSubmitting, setIsSubmitting] = useState(false);

  const isNameValid = name.trim().length >= 2 && name.trim().length <= 60;
  const isPrefixValid =
    !prefix || /^[A-Z]{2,6}$/.test(prefix);
  const isStep1Valid = isNameValid && isPrefixValid;
  const isStep2Valid = !!icon && !!color;
  const isStep3Valid = isNameValid && isPrefixValid && isStep2Valid;

  const handlePrevious = () => {
    setStep(Math.max(1, step - 1));
  };

  const handleNext = () => {
    if (step < 3) {
      setStep(step + 1);
    }
  };

  const handleCancel = () => {
    resetWizard();
    onOpenChange(false);
  };

  const handleCreate = async () => {
    if (!isStep3Valid) return;

    setIsSubmitting(true);
    try {
      const result = await createProjectMutation.mutateAsync({
        name: name.trim(),
        prefix: prefix || name.slice(0, 3).toUpperCase(),
        icon,
        color,
        idempotencyKey: ulid(),
      });

      resetWizard();
      onOpenChange(false);
      navigate(`/p/${result.id}`);
    } catch (err) {
      // Error is shown in review step, allow user to go back and retry
      console.error("Failed to create project:", err);
    } finally {
      setIsSubmitting(false);
    }
  };

  const resetWizard = () => {
    setStep(1);
    setName("");
    setPrefix("");
    setIcon("palette");
    setColor("#fa520f");
    createProjectMutation.reset();
  };

  const getTitle = () => {
    const titles = ["New project — Name", "New project — Appearance", "New project — Review"];
    return titles[step - 1];
  };

  return (
    <Dialog
      open={open}
      onClose={handleCancel}
      title={getTitle()}
      actions={
        <div className={actionsStyle}>
          <span className={stepIndicatorStyle}>
            Step {step} of 3
          </span>
          <div className={buttonsStyle}>
            {step > 1 ? (
              <Button
                variant="ghost"
                onClick={handlePrevious}
                disabled={isSubmitting}
              >
                Back
              </Button>
            ) : (
              <Button
                variant="ghost"
                onClick={handleCancel}
                disabled={isSubmitting}
              >
                Cancel
              </Button>
            )}
            {step < 3 ? (
              <Button
                variant="primary"
                onClick={handleNext}
                disabled={
                  isSubmitting ||
                  (step === 1 ? !isStep1Valid : !isStep2Valid)
                }
              >
                Next
              </Button>
            ) : (
              <Button
                variant="primary"
                onClick={handleCreate}
                disabled={isSubmitting || !isStep3Valid}
              >
                {isSubmitting ? "Creating..." : "Create project"}
              </Button>
            )}
          </div>
        </div>
      }
    >
      <div className={bodyStyle}>
        {step === 1 && (
          <NameStep
            name={name}
            onNameChange={setName}
            prefix={prefix}
            onPrefixChange={setPrefix}
          />
        )}

        {step === 2 && (
          <AppearanceStep
            icon={icon}
            onIconChange={setIcon}
            color={color}
            onColorChange={setColor}
          />
        )}

        {step === 3 && (
          <ReviewStep
            name={name}
            prefix={prefix}
            icon={icon}
            color={color}
            error={createProjectMutation.error}
          />
        )}
      </div>
    </Dialog>
  );
}

const bodyStyle = css({
  padding: "20px 0",
});

const actionsStyle = css({
  display: "flex",
  alignItems: "center",
  gap: "16px",
});

const stepIndicatorStyle = css({
  fontSize: "12px",
  color: "var(--beam-color-text-tertiary)",
});

const buttonsStyle = css({
  display: "flex",
  gap: "8px",
  marginLeft: "auto",
});
