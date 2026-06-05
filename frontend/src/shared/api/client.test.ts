import { afterEach, describe, expect, it, vi } from "vitest";

import { api } from "./client";

describe("api client", () => {
  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("posts run creation requests to the FastAPI endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ pipeline_run_id: 12, task_id: "task-12", status: "queued" }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await api.createRun({
      query: "dataset reuse",
      limit: 5,
      open_access_only: true,
      overwrite: false,
      um_datasets_path: "data/um_datasets.csv",
      output_path: "storage/exports/insights.csv",
      strategy: "high_throughput",
    });

    expect(response.pipeline_run_id).toBe(12);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/runs",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          query: "dataset reuse",
          limit: 5,
          open_access_only: true,
          overwrite: false,
          um_datasets_path: "data/um_datasets.csv",
          output_path: "storage/exports/insights.csv",
          strategy: "high_throughput",
        }),
      }),
    );
  });

  it("builds run event polling requests with a limit", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ events: [{ id: 1, pipeline_run_id: 12, message: "created" }] }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const events = await api.runEvents(12, 50);

    expect(events).toHaveLength(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/runs/12/events?limit=50",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("posts reset requests to the guarded admin endpoint", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({
        status: "successful",
        active_runs: 0,
        truncated_tables: ["pipeline_runs"],
        deleted_paths: [],
        recreated_directories: ["storage/pdf"],
      }),
    });
    vi.stubGlobal("fetch", fetchMock);

    const response = await api.reset({ confirm: "RESET DATASIGHT", force: true });

    expect(response.status).toBe("successful");
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/admin/reset",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ confirm: "RESET DATASIGHT", force: true }),
      }),
    );
  });

  it("raises ApiError with reset failure details", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: false,
      status: 409,
      statusText: "Conflict",
      json: async () => ({ detail: "1 pipeline run(s) are still active." }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await expect(api.reset({ confirm: "RESET DATASIGHT", force: false })).rejects.toMatchObject({
      status: 409,
      detail: "1 pipeline run(s) are still active.",
    });
  });
});
