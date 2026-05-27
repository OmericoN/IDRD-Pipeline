import * as React from "react";

import { cn } from "../../lib/utils";

export type InputProps = React.InputHTMLAttributes<HTMLInputElement>;

export const Input = React.forwardRef<HTMLInputElement, InputProps>(({ className, ...props }, ref) => (
  <input
    ref={ref}
    className={cn(
      "h-9 w-full rounded-md border border-[var(--input)] bg-[var(--surface)] px-3 text-sm text-[var(--foreground)] outline-none transition-colors placeholder:text-neutral-400 focus:border-[var(--ring)] focus:ring-2 focus:ring-teal-100 disabled:cursor-not-allowed disabled:opacity-50",
      className,
    )}
    {...props}
  />
));
Input.displayName = "Input";
