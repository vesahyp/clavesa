/**
 * WidgetTypePicker — choose a widget type when adding a widget.
 *
 * A dialog of the four widget types with an icon and a one-line
 * description, so the author picks intent first; the editor then seeds a
 * sensibly-sized widget of that type.
 */

import {
  Activity,
  BarChart3,
  CircleSlash,
  Hash,
  Layers,
  LineChart,
  PieChart,
  Table,
} from "lucide-react";

import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog";

export type WidgetType =
  | "big_number"
  | "line"
  | "bar"
  | "stacked_bar"
  | "bar_line"
  | "pie"
  | "donut"
  | "table";

const TYPES: {
  type: WidgetType;
  label: string;
  hint: string;
  Icon: typeof Hash;
}[] = [
  {
    type: "big_number",
    label: "Big number",
    hint: "A single headline metric.",
    Icon: Hash,
  },
  {
    type: "line",
    label: "Line chart",
    hint: "A value over an ordered axis.",
    Icon: LineChart,
  },
  {
    type: "bar",
    label: "Bar chart",
    hint: "A value compared across categories.",
    Icon: BarChart3,
  },
  {
    type: "stacked_bar",
    label: "Stacked bar",
    hint: "Several value columns stacked per x.",
    Icon: Layers,
  },
  {
    type: "bar_line",
    label: "Bar + line",
    hint: "A bar metric and a line metric, dual axis.",
    Icon: Activity,
  },
  {
    type: "pie",
    label: "Pie chart",
    hint: "Share of total across categories.",
    Icon: PieChart,
  },
  {
    type: "donut",
    label: "Donut chart",
    hint: "Pie with a hollow center.",
    Icon: CircleSlash,
  },
  {
    type: "table",
    label: "Table",
    hint: "The raw query result, all columns.",
    Icon: Table,
  },
];

interface WidgetTypePickerProps {
  open: boolean;
  onOpenChange: (open: boolean) => void;
  onPick: (type: WidgetType) => void;
}

export function WidgetTypePicker({
  open,
  onOpenChange,
  onPick,
}: WidgetTypePickerProps) {
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Add a widget</DialogTitle>
          <DialogDescription>Pick a type to start with.</DialogDescription>
        </DialogHeader>
        <div className="grid grid-cols-2 gap-3">
          {TYPES.map(({ type, label, hint, Icon }) => (
            <button
              key={type}
              type="button"
              onClick={() => onPick(type)}
              className="flex flex-col gap-1 rounded-md border border-border bg-card p-4 text-left transition-colors hover:border-primary hover:bg-muted"
            >
              <Icon className="h-5 w-5 text-muted-foreground" />
              <span className="text-sm font-medium">{label}</span>
              <span className="text-xs text-muted-foreground">{hint}</span>
            </button>
          ))}
        </div>
      </DialogContent>
    </Dialog>
  );
}
