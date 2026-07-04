import * as React from "react";

import { cn } from "@/lib/utils";

/**
 * Base form-field classes shared by Input and NativeSelect
 * (ui/native-select.tsx) so the two widgets render identically —
 * border, focus ring, disabled styling and all.
 */
export const inputBaseClasses =
  "flex h-9 w-full rounded-md border border-input bg-background px-3 py-1 text-sm shadow-sm transition-colors placeholder:text-muted-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50";

export const Input = React.forwardRef<
  HTMLInputElement,
  React.InputHTMLAttributes<HTMLInputElement>
>(({ className, type, ...props }, ref) => (
  <input
    type={type}
    ref={ref}
    className={cn(
      inputBaseClasses,
      "file:border-0 file:bg-transparent file:text-sm file:font-medium",
      className
    )}
    {...props}
  />
));
Input.displayName = "Input";
