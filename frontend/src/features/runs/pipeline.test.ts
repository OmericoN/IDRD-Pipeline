import { describe, expect, it } from "vitest";

import type { PipelineRunSummary, StageInfo } from "../../shared/api/client";
import {
  activeParallelStages,
  edgeToneFromSourceStatus,
  mergeStages,
  runStrategyLabel,
  stageProgress,
  stageStatusCounts,
  statusTone,
} from "./pipeline";

const stages: StageInfo[] = [
  { name: "discover", description: "Find publication metadata." },
  { name: "download_pdf", description: "Download PDFs." },
  { name: "export_insights", description: "Export CSV." },
];

describe("pipeline view helpers", () => {
  it("marks the next missing stage as running while a run is active", () => {
    const run: PipelineRunSummary = {
      id: 1,
      run_key: "run-1",
      query: "dataset reuse",
      status: "running",
      config: {},
      celery_task_id: null,
      error: null,
      created_at: null,
      updated_at: null,
      finished_at: null,
      stages: [
        {
          id: 1,
          stage: "discover",
          status: "successful",
          attempt_count: 1,
          task_id: null,
          error: null,
          metrics: {},
          started_at: null,
          finished_at: null,
          created_at: null,
          updated_at: null,
        },
      ],
    };

    const visual = mergeStages(stages, run);

    expect(visual.map((stage) => stage.status)).toEqual(["successful", "running", "pending"]);
  });

  it("computes completion from terminal stage states", () => {
    const visual = [
      { ...stages[0]!, status: "successful", working: false },
      { ...stages[1]!, status: "skipped", working: false },
      { ...stages[2]!, status: "running", working: true },
    ];

    expect(stageProgress(visual)).toBe(67);
  });

  it("counts completed-with-errors stages as complete", () => {
    const visual = [
      { ...stages[0]!, status: "completed_with_errors", working: false },
      { ...stages[1]!, status: "pending", working: false },
      { ...stages[2]!, status: "pending", working: false },
    ];

    expect(stageProgress(visual)).toBe(33);
  });

  it("counts completed-with-errors stages in the error summary", () => {
    const visual = [
      { ...stages[0]!, status: "successful", working: false },
      { ...stages[1]!, status: "completed_with_errors", working: false },
      { ...stages[2]!, status: "skipped", working: false },
    ];

    expect(stageStatusCounts(visual)).toEqual({ done: 1, skipped: 1, errors: 1 });
  });

  it("supports the legacy error status in the error summary", () => {
    const visual = [
      { ...stages[0]!, status: "failed", working: false },
      { ...stages[1]!, status: "error", working: false },
      { ...stages[2]!, status: "running", working: true },
    ];

    expect(stageStatusCounts(visual)).toEqual({ done: 0, skipped: 0, errors: 2 });
  });

  it("preserves multiple recorded running stages for high-throughput runs", () => {
    const run: PipelineRunSummary = {
      id: 1,
      run_key: "run-1",
      query: "dataset reuse",
      status: "running",
      config: { strategy: "high_throughput" },
      celery_task_id: null,
      error: null,
      created_at: null,
      updated_at: null,
      finished_at: null,
      stages: [
        {
          id: 1,
          stage: "download_pdf",
          status: "running",
          attempt_count: 1,
          task_id: null,
          error: null,
          metrics: {},
          started_at: null,
          finished_at: null,
          created_at: null,
          updated_at: null,
        },
        {
          id: 2,
          stage: "grobid_convert",
          status: "running",
          attempt_count: 1,
          task_id: null,
          error: null,
          metrics: {},
          started_at: null,
          finished_at: null,
          created_at: null,
          updated_at: null,
        },
      ],
    };

    const visual = mergeStages(stages, run);

    expect(visual.map((stage) => stage.working)).toEqual([false, true, false]);
    expect(visual[1]?.status).toBe("running");
  });

  it.each([
    ["failed", "error"],
    ["error", "error"],
    ["skipped", "warning"],
    ["warning", "warning"],
    ["successful", "success"],
    ["running", "success"],
    ["queued", "success"],
    ["started", "success"],
    ["pending", "muted"],
  ] as const)("maps %s source status to %s edge tone", (status, tone) => {
    expect(edgeToneFromSourceStatus(status)).toBe(tone);
  });

  it("treats completed_with_errors as a warning status", () => {
    expect(statusTone("completed_with_errors")).toBe("warning");
  });

  it("labels run strategy from config", () => {
    expect(
      runStrategyLabel({
        id: 1,
        run_key: "run-1",
        query: "dataset reuse",
        status: "running",
        config: { strategy: "high_throughput" },
        celery_task_id: null,
        error: null,
        created_at: null,
        updated_at: null,
        finished_at: null,
        stages: [],
      }),
    ).toBe("High-throughput");
  });

  it("returns active parallel stages only for high-throughput runs", () => {
    const run: PipelineRunSummary = {
      id: 1,
      run_key: "run-1",
      query: "dataset reuse",
      status: "running",
      config: { strategy: "high_throughput" },
      celery_task_id: null,
      error: null,
      created_at: null,
      updated_at: null,
      finished_at: null,
      stages: [],
    };
    const visual = [
      { ...stages[0]!, status: "running", working: true },
      { ...stages[1]!, status: "queued", working: true },
      { ...stages[2]!, status: "pending", working: false },
    ];

    expect(activeParallelStages(run, visual).map((stage) => stage.name)).toEqual([
      "discover",
      "download_pdf",
    ]);
  });
});
