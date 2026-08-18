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
      preview_id: "preview-12",
      processing_limit: 5,
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
          preview_id: "preview-12",
          processing_limit: 5,
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

  it("builds encoded UM dataset catalog requests", async () => {
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      json: async () => ({ items: [], total: 0, offset: 50, limit: 50, repositories: [], years: [] }),
    });
    vi.stubGlobal("fetch", fetchMock);

    await api.umDatasets({ q: "health data", repository: "Harvard Dataverse", year: 2024, offset: 50 });

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/um-datasets?q=health+data&repository=Harvard+Dataverse&year=2024&offset=50&limit=50",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("requests verification and encoded UM dataset details", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({}) });
    vi.stubGlobal("fetch", fetchMock);

    await api.verifyUmDatasets();
    await api.umDataset("W/123");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/um-datasets/verification",
      expect.objectContaining({ method: "GET" }),
    );
    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/um-datasets/W%2F123",
      expect.objectContaining({ method: "GET" }),
    );
  });

  it("imports an authoritative UM dataset directory", async () => {
    const fetchMock = vi.fn().mockResolvedValue({ ok: true, json: async () => ({ count: 2748 }) });
    vi.stubGlobal("fetch", fetchMock);

    await api.importUmDatasets("data/um_dataset");

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/um-datasets/import",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ path: "data/um_dataset" }),
      }),
    );
  });

  it("downloads a selected-column insight CSV with its server filename", async () => {
    const blob = new Blob(["paper_id\nW123\n"], { type: "text/csv" });
    const fetchMock = vi.fn().mockResolvedValue({
      ok: true,
      headers: {
        get: (name: string) =>
          name === "Content-Disposition"
            ? 'attachment; filename="datasight-insights.csv"'
            : null,
      },
      blob: async () => blob,
    });
    vi.stubGlobal("fetch", fetchMock);

    const result = await api.downloadInsightsCsv(["paper_id", "discovery_mode"]);

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/insights/export.csv?columns=paper_id&columns=discovery_mode",
      { method: "GET" },
    );
    expect(result).toEqual({ blob, filename: "datasight-insights.csv" });
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
