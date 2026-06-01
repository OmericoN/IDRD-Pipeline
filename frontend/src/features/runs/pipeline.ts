import type { PipelineRunSummary, StageInfo, StageRunSummary } from "../../shared/api/client";
import { isActiveRun } from "../../shared/api/client";

export type VisualStage = StageInfo & {
  status: string;
  run?: StageRunSummary;
};

const COMPLETE_STATUSES = new Set(["successful", "failed", "skipped"]);
const ACTIVE_STATUSES = new Set(["queued", "running", "started"]);

export function mergeStages(stages: StageInfo[], run: PipelineRunSummary | undefined): VisualStage[] {
  const active = isActiveRun(run?.status);
  let markedActive = false;

  return stages.map((stage) => {
    const recorded = run?.stages.find((item) => item.stage === stage.name);
    if (recorded) {
      return { ...stage, status: recorded.status, run: recorded };
    }
    if (active && !markedActive) {
      markedActive = true;
      return { ...stage, status: "running" };
    }
    return { ...stage, status: "pending" };
  });
}

export function stageProgress(stages: VisualStage[]) {
  if (!stages.length) {
    return 0;
  }
  const complete = stages.filter((stage) => COMPLETE_STATUSES.has(stage.status)).length;
  return Math.round((complete / stages.length) * 100);
}

export function formatStageName(value: string) {
  return value.replaceAll("_", " ");
}

export type StatusTone = "default" | "success" | "warning" | "error" | "muted";

export function isTerminalStatus(status: string) {
  return COMPLETE_STATUSES.has(status);
}

export function isWorkingStatus(status: string) {
  return ACTIVE_STATUSES.has(status);
}

export function statusTone(status: string): StatusTone {
  if (status === "successful") {
    return "success";
  }
  if (status === "failed" || status === "error") {
    return "error";
  }
  if (status === "skipped" || status === "warning") {
    return "warning";
  }
  if (status === "pending") {
    return "muted";
  }
  return "default";
}

export function statusBadgeVariant(status: string): "default" | "secondary" | "destructive" | "outline" {
  const tone = statusTone(status);
  if (tone === "error") {
    return "destructive";
  }
  if (tone === "muted") {
    return "secondary";
  }
  if (tone === "warning" || tone === "success") {
    return "outline";
  }
  return "default";
}
