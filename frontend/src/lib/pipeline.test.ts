import { describe, expect, it } from "vitest";

import type { PipelineRunSummary, StageInfo } from "./api";
import { mergeStages, stageProgress } from "./pipeline";

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
      { ...stages[0]!, status: "successful" },
      { ...stages[1]!, status: "skipped" },
      { ...stages[2]!, status: "running" },
    ];

    expect(stageProgress(visual)).toBe(67);
  });
});
