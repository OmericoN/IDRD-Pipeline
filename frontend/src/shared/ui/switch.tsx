import * as React from "react";

import { cn } from "../lib/utils";

type SwitchProps = Omit<React.InputHTMLAttributes<HTMLInputElement>, "type">;

export const Switch = React.forwardRef<HTMLInputElement, SwitchProps>(
  ({ className, checked, ...props }, ref) => (
    <label
      className={cn(
        "relative inline-flex h-5 w-9 shrink-0 cursor-pointer items-center rounded-full bg-neutral-300 transition-colors has-[:checked]:bg-[var(--accent)]",
        className,
      )}
    >
      <input ref={ref} type="checkbox" className="peer sr-only" checked={checked} {...props} />
      <span className="pointer-events-none ml-0.5 h-4 w-4 rounded-full bg-white transition-transform peer-checked:translate-x-4" />
    </label>
  ),
);
Switch.displayName = "Switch";
