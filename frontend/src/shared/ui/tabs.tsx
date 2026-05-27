import * as React from "react";

import { cn } from "../lib/utils";

type TabsContextValue = {
  value: string;
  onValueChange: (value: string) => void;
};

const TabsContext = React.createContext<TabsContextValue | null>(null);

export function Tabs({
  value,
  onValueChange,
  children,
}: {
  value: string;
  onValueChange: (value: string) => void;
  children: React.ReactNode;
}) {
  return <TabsContext.Provider value={{ value, onValueChange }}>{children}</TabsContext.Provider>;
}

export function TabsList({ className, ...props }: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      className={cn("inline-flex h-9 items-center rounded-md bg-[var(--secondary)] p-1", className)}
      {...props}
    />
  );
}

export function TabsTrigger({
  value,
  className,
  ...props
}: React.ButtonHTMLAttributes<HTMLButtonElement> & { value: string }) {
  const context = React.useContext(TabsContext);
  const active = context?.value === value;
  return (
    <button
      type="button"
      className={cn(
        "inline-flex h-7 items-center justify-center rounded px-3 text-xs font-medium text-[var(--muted-foreground)] transition-colors hover:text-[var(--foreground)]",
        active && "bg-[var(--surface)] text-[var(--foreground)] shadow-sm",
        className,
      )}
      onClick={() => context?.onValueChange(value)}
      {...props}
    />
  );
}

export function TabsContent({
  value,
  className,
  ...props
}: React.HTMLAttributes<HTMLDivElement> & { value: string }) {
  const context = React.useContext(TabsContext);
  if (context?.value !== value) {
    return null;
  }
  return <div className={cn("mt-3", className)} {...props} />;
}
