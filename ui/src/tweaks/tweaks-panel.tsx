import { ReactNode } from "react";
import { Dialog } from "@sunbeam/beam-ui";
import { TweakSection, TweakRadio, TweakToggle } from "@sunbeam/beam-ui";
import { useTweaks } from "./store";
import { css } from "styled-system/css";

/** Props for {@link TweaksPanel}. */
interface TweaksPanelProps {
  /** Whether the panel is open. */
  open: boolean;
  /** Called when the user closes the panel. */
  onOpenChange: (open: boolean) => void;
}

/**
 * Tweaks panel component.
 *
 * Renders a modal dialog with tweak controls composing beam-ui primitives.
 * Sections: Appearance (theme, density, accents) and Board (showWip, groupBy).
 *
 * @example
 * ```tsx
 * const [tweaksOpen, setTweaksOpen] = useState(false);
 * return (
 *   <>
 *     <button onClick={() => setTweaksOpen(true)}>Settings</button>
 *     <TweaksPanel open={tweaksOpen} onOpenChange={setTweaksOpen} />
 *   </>
 * );
 * ```
 */
export function TweaksPanel({ open, onOpenChange }: TweaksPanelProps): ReactNode {
  const { tweaks, setTweak } = useTweaks();

  return (
    <Dialog open={open} onClose={() => onOpenChange(false)} title="Settings">
      <div className={panelContainer}>
        <div className={panelContent}>
          <TweakSection label="Appearance">
            <TweakRadio
              label="Theme"
              value={tweaks.theme}
              options={[
                { value: "light", label: "Light" },
                { value: "dark", label: "Dark" },
              ]}
              onChange={(v) => setTweak("theme", v as "light" | "dark")}
            />
            <TweakRadio
              label="Density"
              value={tweaks.density}
              options={[
                { value: "cozy", label: "Cozy" },
                { value: "compact", label: "Compact" },
              ]}
              onChange={(v) => setTweak("density", v as "cozy" | "compact")}
            />
            <TweakToggle
              label="Column accents"
              value={tweaks.accents}
              onChange={(v) => setTweak("accents", v)}
            />
          </TweakSection>

          <TweakSection label="Board">
            <TweakToggle
              label="Show WIP limits"
              value={tweaks.showWip}
              onChange={(v) => setTweak("showWip", v)}
            />
            <TweakRadio
              label="Group cards by"
              value={tweaks.groupBy}
              options={[
                { value: "status", label: "Status" },
                { value: "assignee", label: "Assignee" },
                { value: "priority", label: "Priority" },
              ]}
              onChange={(v) =>
                setTweak("groupBy", v as "status" | "assignee" | "priority")
              }
            />
          </TweakSection>
        </div>
      </div>
    </Dialog>
  );
}

const panelContainer = css({
  display: "flex",
  flexDirection: "column",
  gap: "20px",
  maxWidth: "400px",
  padding: "20px",
});

const panelHeader = css({
  display: "flex",
  alignItems: "center",
  justifyContent: "space-between",
  borderBottomWidth: "1px",
  borderBottomStyle: "solid",
  borderBottomColor: "border.default",
  paddingBottom: "12px",
});

const panelTitle = css({
  fontSize: "16px",
  fontWeight: 600,
  color: "text.primary",
  margin: 0,
});

const panelContent = css({
  display: "flex",
  flexDirection: "column",
  gap: "4px",
});
