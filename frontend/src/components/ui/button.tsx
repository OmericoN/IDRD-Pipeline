import * as React from "react";
import { type VariantProps, cva } from "class-variance-authority";

import { cn } from "../../lib/utils";

const buttonVariants = cva(
  "inline-flex h-9 items-center justify-center gap-2 rounded-md px-3 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-[var(--ring)] disabled:pointer-events-none disabled:opacity-50",
  {
    variants: {
      variant: {
        default: "bg-[var(--primary)] text-[var(--primary-foreground)] hover:bg-[#203632]",
        secondary: "bg-[var(--secondary)] text-[var(--secondary-foreground)] hover:bg-[#d8e2df]",
        ghost: "text-[var(--foreground)] hover:bg-[var(--muted)]",
        outline: "border border-[var(--border)] bg-[var(--surface)] text-[var(--foreground)] hover:bg-[var(--surface-hover)]",
        destructive: "bg-[var(--destructive)] text-[var(--destructive-foreground)] hover:bg-red-800",
      },
      size: {
        default: "h-9 px-3",
        sm: "h-8 px-2.5 text-xs",
        icon: "h-9 w-9 px-0",
      },
    },
    defaultVariants: {
      variant: "default",
      size: "default",
    },
  },
);

export type ButtonProps = React.ButtonHTMLAttributes<HTMLButtonElement> &
  VariantProps<typeof buttonVariants>;

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  ({ className, variant, size, ...props }, ref) => (
    <button className={cn(buttonVariants({ variant, size }), className)} ref={ref} {...props} />
  ),
);
Button.displayName = "Button";
