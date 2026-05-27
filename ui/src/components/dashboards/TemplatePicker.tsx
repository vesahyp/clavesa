/**
 * TemplatePicker — the modal shown on `?new=1` before the editor opens.
 *
 * Four cards: Blank + three opinionated starting shapes. Selection
 * materialises the corresponding spec via `buildTemplate` and calls
 * back; Dashboard.tsx then seeds the editor with it. Cancel routes
 * back to the dashboards list — the user opted in to creating a new
 * dashboard, and changing their mind shouldn't drop them in the
 * editor.
 */

import {
  Activity,
  FileText,
  Hash,
  LineChart,
  Sparkles,
} from "lucide-react";

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

import { TEMPLATES, type TemplateId } from "./templates";

const ICONS: Record<TemplateId, typeof Hash> = {
  blank: Sparkles,
  scoreboard: Hash,
  line_bar: Activity,
  top_n: FileText,
};

interface TemplatePickerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onPick: (id: TemplateId) => void;
}

export function TemplatePicker({
  open,
  onOpenChange,
  onPick,
}: TemplatePickerProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Start your dashboard</DialogTitle>
          <DialogDescription>
            Pick a starting shape or begin blank.
          </DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-3">
          {TEMPLATES.map((t) => {
            const Icon = ICONS[t.id] ?? LineChart;
            return (
              <button
                key={t.id}
                type="button"
                onClick={() => onPick(t.id)}
                className="flex flex-col gap-1 rounded-md border border-border bg-card p-4 text-left transition-colors hover:border-primary hover:bg-muted"
              >
                <Icon className="h-5 w-5 text-muted-foreground" />
                <span className="text-sm font-medium">{t.label}</span>
                <span className="text-xs text-muted-foreground">{t.hint}</span>
              </button>
            );
          })}
        </div>
      </DialogContent>
    </Dialog>
  );
}
