/**
 * NativeSelect — a native <select> styled like shadcn's Input. Used over
 * the Radix-based Select primitive when we need testability via
 * getByLabelText().value (the Radix one is a custom button, not a native
 * select). Density overrides (h-8, text-xs, …) go through className;
 * `cn` (tailwind-merge) resolves the conflicts against the base list.
 */
import * as React from "react";

import { cn } from "@/lib/utils";
import { inputBaseClasses } from "@/components/ui/input";

export const NativeSelect = React.forwardRef<
  HTMLSelectElement,
  React.SelectHTMLAttributes<HTMLSelectElement>
>(({ className, ...props }, ref) => (
  <select ref={ref} className={cn(inputBaseClasses, className)} {...props} />
));
NativeSelect.displayName = "NativeSelect";
