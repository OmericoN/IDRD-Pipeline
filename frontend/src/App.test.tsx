import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./app/App";
import { TooltipProvider } from "./components/ui/tooltip";

function renderApp(fetchMock: ReturnType<typeof vi.fn>, initialPath = "/") {
  vi.stubGlobal("fetch", fetchMock);
  window.history.pushState({}, "", initialPath);
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <TooltipProvider>
        <App />
      </TooltipProvider>
    </QueryClientProvider>,
  );
}

function response(body: unknown, ok = true, status = 200, statusText = "OK") {
  return {
    ok,
    status,
    statusText,
    json: async () => body,
  };
}

const insightColumns = ["paper_id", "discovery_mode", "discovery_methods"];

const discoveryPhases = [
  ["direct", "Direct evidence", 2748, 100, 56, 0.0056],
  ["exact", "Exact mentions", 2848, 100, 130, 0.13],
  ["related", "Related works", 5284, 100, 53, 0.0053],
  ["focused", "Focused expansion", 54, 2, 7, 0.007],
].map(([id, label, coverage_count, coverage_percent, estimated_calls, estimated_cost_usd]) => ({
  id,
  label,
  description: `${label} explanation`,
  coverage_count,
  coverage_percent,
  estimated_calls,
  estimated_cost_usd,
}));

const discoveryProfile = {
  dataset_count: 2748,
  catalog_fingerprint: "catalog-123",
  coverage: { openalex_ids: 100, dois: 100, keywords: 98.5, topics: 56.1, related_works: 35.1 },
  counts: { openalex_ids: 2748, dois: 2748, unique_related_works: 5284, informative_keywords: 42, topic_names: 12, resolved_topics: 9 },
  topic_resolution: { status: "partial", resolved: 9, requested: 12, unresolved: 3 },
  phases: discoveryPhases,
  top_topics: ["Population Health"],
  top_keywords: ["longitudinal cohort"],
  warnings: ["3 priority topic names still need a strong OpenAlex match."],
};

const discoveryPreview = {
  preview_id: "preview-123",
  strategy_version: 2,
  strategy_fingerprint: "strategy-123",
  catalog_fingerprint: "catalog-123",
  expires_at: "2026-08-04T12:30:00Z",
  candidate_count: 143,
  included_count: 1,
  ready_count: 81,
  watchlist_count: 62,
  estimated_cost_usd: 0.148,
  actual_cost_usd: 0.041,
  actual_calls: 41,
  max_cost_usd: 0.25,
  partial: false,
  rate_limit: { remaining: "959" },
  stop_reason: "ready_target_met",
  completed_phases: ["direct"],
  phase_results: { direct: { status: "completed", fetched: 42, unique_added: 40, ready_after_phase: 81, calls: 2, cost_usd: 0.002 } },
  warnings: [],
  metrics: { target_met: true, unique_fetched: 143, duplicates_seen: 3 },
  profile: discoveryProfile,
  candidates: [
    {
      paper_id: "W900",
      title: "Secondary analysis of a Maastricht longitudinal cohort",
      doi: "10.1234/reuse",
      year: 2025,
      source_url: "https://openalex.org/W900",
      open_access_url: "https://example.org/paper.pdf",
      oa_status: "green",
      cited_by_count: 7,
      primary_source_name: "Research Data Journal",
      candidate_strength: 98,
      evidence_tier: "direct",
      evidence_reasons: ["dataset_citation", "identifier_mention"],
      matched_um_dataset_ids: ["W123"],
      pipeline_ready: true,
      included: true,
      exclusion_reason: null,
    },
    {
      paper_id: "W901",
      title: "Potential reuse without downloadable full text",
      doi: null,
      year: 2024,
      source_url: "https://openalex.org/W901",
      open_access_url: null,
      oa_status: "closed",
      cited_by_count: 2,
      primary_source_name: null,
      candidate_strength: 75,
      evidence_tier: "exact",
      evidence_reasons: ["title_mention"],
      matched_um_dataset_ids: ["W123"],
      pipeline_ready: false,
      included: false,
      exclusion_reason: "No usable PDF link",
    },
  ],
};

function defaultFetchMock(resetResponse = response({
  status: "successful",
  active_runs: 0,
  truncated_tables: ["pipeline_runs"],
  deleted_paths: [],
  recreated_directories: ["storage/pdf"],
})) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const path = String(input);
    if (path === "/api/v1/health") {
      return response({
        database_ready: true,
        redis_ready: true,
        worker_ready: true,
        grobid_ready: true,
        details: {},
      });
    }
    if (path === "/api/v1/stages") {
      return response({
        stages: [
          { name: "discover", description: "Find publication metadata." },
          { name: "download_pdf", description: "Download PDFs." },
          { name: "grobid_convert", description: "Convert PDFs." },
          { name: "render_document", description: "Render text." },
          { name: "detect_mentions", description: "Detect mentions." },
          { name: "extract_features", description: "Extract features." },
          { name: "match_um_dataset", description: "Match UM datasets." },
          { name: "export_insights", description: "Export insights." },
        ],
      });
    }
    if (path.startsWith("/api/v1/runs?")) {
      return response({
        runs: [
          {
            id: 39,
            run_key: "run-39",
            query: "Maastricht dataset reuse",
            status: "running",
            config: { strategy: "high_throughput" },
            celery_task_id: null,
            error: null,
            created_at: "2026-05-31T19:00:00Z",
            updated_at: "2026-05-31T19:02:00Z",
            finished_at: null,
            stages: [
              {
                id: 1,
                stage: "discover",
                status: "successful",
                attempt_count: 1,
                task_id: null,
                error: null,
                metrics: { count: 12 },
                started_at: null,
                finished_at: null,
                created_at: null,
                updated_at: null,
              },
              {
                id: 2,
                stage: "download_pdf",
                status: "running",
                attempt_count: 1,
                task_id: null,
                error: null,
                metrics: { active: 2 },
                started_at: null,
                finished_at: null,
                created_at: null,
                updated_at: null,
              },
              {
                id: 3,
                stage: "grobid_convert",
                status: "running",
                attempt_count: 1,
                task_id: null,
                error: null,
                metrics: { active: 1 },
                started_at: null,
                finished_at: null,
                created_at: null,
                updated_at: null,
              },
            ],
          },
        ],
      });
    }
    if (path === "/api/v1/runs/39" || path === "/api/v1/runs/40") {
      const runId = path.endsWith("/40") ? 40 : 39;
      return response({
        id: runId,
        run_key: `run-${runId}`,
        query: "Maastricht dataset reuse",
        status: "running",
        config: { strategy: "high_throughput" },
        celery_task_id: null,
        error: null,
        created_at: "2026-05-31T19:00:00Z",
        updated_at: "2026-05-31T19:02:00Z",
        finished_at: null,
        stages: [
          {
            id: 1,
            stage: "discover",
            status: "successful",
            attempt_count: 1,
            task_id: null,
            error: null,
            metrics: { count: 12 },
            started_at: null,
            finished_at: null,
            created_at: null,
            updated_at: null,
          },
          {
            id: 2,
            stage: "download_pdf",
            status: "running",
            attempt_count: 1,
            task_id: null,
            error: null,
            metrics: { active: 2 },
            started_at: null,
            finished_at: null,
            created_at: null,
            updated_at: null,
          },
          {
            id: 3,
            stage: "grobid_convert",
            status: "running",
            attempt_count: 1,
            task_id: null,
            error: null,
            metrics: { active: 1 },
            started_at: null,
            finished_at: null,
            created_at: null,
            updated_at: null,
          },
        ],
      });
    }
    if (path.startsWith("/api/v1/runs/39/events") || path.startsWith("/api/v1/runs/40/events")) {
      const runId = path.includes("/runs/40/") ? 40 : 39;
      return response({
        events: [
          {
            id: 1,
            pipeline_run_id: runId,
            stage: "discover",
            level: "info",
            message: "Discovery started",
            payload: { query: "Maastricht dataset reuse" },
            created_at: "2026-05-31T19:00:00Z",
          },
        ],
      });
    }
    if (path.startsWith("/api/v1/insights?")) {
      return response({ columns: insightColumns, rows: [] });
    }
    if (path === "/api/v1/discovery/um-profile") {
      return response(discoveryProfile);
    }
    if (path === "/api/v1/openalex/status") {
      return response({
        status: "ready",
        available: true,
        remaining: 959,
        limit: 1000,
        reset_seconds: 3600,
        message: "OpenAlex is ready.",
      });
    }
    if (path === "/api/v1/discovery/preview" && init?.method === "POST") {
      return response(discoveryPreview);
    }
    if (path === "/api/v1/um-datasets?offset=0&limit=50") {
      return response({
        items: [
          {
            um_dataset_id: "W123",
            title: "Maastricht Health Dataset",
            aliases: ["MHD"],
            creators: ["Jane Doe"],
            doi: "10.1234/health",
            url: "https://example.org/dataset",
            year: 2024,
            repository: "Dataverse",
            keywords: ["health"],
            created_at: "2026-07-20T00:00:00Z",
            updated_at: "2026-07-21T00:00:00Z",
          },
        ],
        total: 51,
        offset: 0,
        limit: 50,
        repositories: ["Dataverse"],
        years: [2024],
      });
    }
    if (path === "/api/v1/um-datasets?offset=50&limit=50") {
      return response({
        items: [
          {
            um_dataset_id: "W999",
            title: "Final UM Dataset",
            aliases: [],
            creators: [],
            doi: null,
            url: null,
            year: 2023,
            repository: "Zenodo",
            keywords: [],
            created_at: null,
            updated_at: null,
          },
        ],
        total: 51,
        offset: 50,
        limit: 50,
        repositories: ["Dataverse", "Zenodo"],
        years: [2024, 2023],
      });
    }
    if (path.startsWith("/api/v1/um-datasets?")) {
      return response({
        items: [
          {
            um_dataset_id: "W123",
            title: "Maastricht Health Dataset",
            aliases: ["MHD"],
            creators: ["Jane Doe"],
            doi: "10.1234/health",
            url: "https://example.org/dataset",
            year: 2024,
            repository: "Dataverse",
            keywords: ["health"],
            created_at: "2026-07-20T00:00:00Z",
            updated_at: "2026-07-21T00:00:00Z",
          },
        ],
        total: 51,
        offset: 0,
        limit: 50,
        repositories: ["Dataverse"],
        years: [2024],
      });
    }
    if (path === "/api/v1/um-datasets/verification") {
      return response({
        status: "verified",
        source_path: "data/um_dataset",
        checked_at: "2026-07-21T00:00:00Z",
        source_count: 1,
        stored_count: 1,
        verified_count: 1,
        issues: [],
        warnings: [],
        metrics: { source_rows: 1 },
        message: null,
      });
    }
    if (path === "/api/v1/um-datasets/W123") {
      return response({
        um_dataset_id: "W123",
        title: "Maastricht Health Dataset",
        aliases: ["MHD"],
        creators: ["Jane Doe"],
        doi: "10.1234/health",
        url: "https://example.org/dataset",
        year: 2024,
        repository: "Dataverse",
        keywords: ["health"],
        raw: { source: "openalex_pure_export" },
        created_at: "2026-07-20T00:00:00Z",
        updated_at: "2026-07-21T00:00:00Z",
      });
    }
    if (path === "/api/v1/admin/reset" && init?.method === "POST") {
      return resetResponse;
    }
    if (path === "/api/v1/runs" && init?.method === "POST") {
      return response({ pipeline_run_id: 40, task_id: "task-40", status: "queued" }, true, 202);
    }
    return response({}, false, 404, "Not Found");
  });
}

describe("App reset dialog", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("keeps reset disabled until exact confirmation and acknowledgement are provided", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/admin");

    const resetButton = await screen.findByRole("button", { name: "Review reset" });
    expect(resetButton).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText("RESET DATASIGHT"), { target: { value: "RESET" } });
    expect(resetButton).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText("RESET DATASIGHT"), { target: { value: "RESET DATASIGHT" } });
    fireEvent.click(screen.getByLabelText("I understand this action cannot be undone from the app."));
    expect(resetButton).toBeEnabled();
  });

  it("submits reset with force setting and shows the success result", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/admin");

    fireEvent.change(screen.getByPlaceholderText("RESET DATASIGHT"), { target: { value: "RESET DATASIGHT" } });
    fireEvent.click(screen.getByLabelText("I understand this action cannot be undone from the app."));
    fireEvent.click(screen.getByLabelText("Force reset active runs"));
    fireEvent.click(screen.getByRole("button", { name: "Review reset" }));
    fireEvent.click(await screen.findByRole("button", { name: "Reset everything" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/admin/reset",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ confirm: "RESET DATASIGHT", force: true }),
        }),
      );
    });
    expect(await screen.findByText(/Reset complete/)).toBeInTheDocument();
  });

  it("explains active-run conflicts without enabling force automatically", async () => {
    const fetchMock = defaultFetchMock(
      response({ detail: "1 pipeline run(s) are still active." }, false, 409, "Conflict"),
    );
    renderApp(fetchMock, "/admin");

    fireEvent.change(screen.getByPlaceholderText("RESET DATASIGHT"), { target: { value: "RESET DATASIGHT" } });
    fireEvent.click(screen.getByLabelText("I understand this action cannot be undone from the app."));
    fireEvent.click(screen.getByRole("button", { name: "Review reset" }));
    fireEvent.click(await screen.findByRole("button", { name: "Reset everything" }));

    expect(await screen.findByText(/Retry only if you are sure by enabling force reset/)).toBeInTheDocument();
    expect(screen.getByLabelText("Force reset active runs")).not.toBeChecked();
  });
});

describe("App routed workflow", () => {
  afterEach(() => {
    cleanup();
    vi.restoreAllMocks();
  });

  it("defaults to the launch page and navigates to runs", async () => {
    renderApp(defaultFetchMock(), "/launch");

    expect(await screen.findByText("Choose a discovery strategy")).toBeInTheDocument();
    expect(screen.getByText("Adaptive funnel")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("link", { name: /^Runs/ }));

    expect(await screen.findByText("Open a run to inspect the workspace, events, and stage metrics.")).toBeInTheDocument();
    expect(screen.getByText("Maastricht dataset reuse")).toBeInTheDocument();
    expect(screen.getByText("High-throughput")).toBeInTheDocument();
  });

  it("downloads all insights with the selected columns", async () => {
    const baseFetch = defaultFetchMock();
    const csvBlob = new Blob(["paper_id,discovery_mode\nW123,catalog_funnel\n"], {
      type: "text/csv",
    });
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (path.startsWith("/api/v1/insights?")) {
        return response({
          columns: insightColumns,
          rows: [
            {
              paper_id: "W123",
              discovery_mode: "catalog_funnel",
              discovery_methods: ["dataset_citation"],
            },
          ],
        });
      }
      if (path.startsWith("/api/v1/insights/export.csv?")) {
        return {
          ok: true,
          status: 200,
          statusText: "OK",
          headers: { get: () => 'attachment; filename="datasight-insights.csv"' },
          blob: async () => csvBlob,
        };
      }
      return baseFetch(input, init);
    });
    const createObjectURL = vi.fn(() => "blob:insights");
    const revokeObjectURL = vi.fn();
    Object.defineProperty(URL, "createObjectURL", { configurable: true, value: createObjectURL });
    Object.defineProperty(URL, "revokeObjectURL", { configurable: true, value: revokeObjectURL });
    vi.spyOn(HTMLAnchorElement.prototype, "click").mockImplementation(() => undefined);
    renderApp(fetchMock, "/insights");

    await screen.findByText("W123");
    fireEvent.click(screen.getByRole("button", { name: "Download CSV" }));
    expect(screen.getByLabelText("Include Paper Id")).toBeChecked();
    expect(screen.getByLabelText("Include Discovery Methods")).toBeChecked();
    fireEvent.click(screen.getByLabelText("Include Discovery Methods"));
    fireEvent.click(screen.getByRole("button", { name: "Download CSV" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/insights/export.csv?columns=paper_id&columns=discovery_mode",
        { method: "GET" },
      );
    });
    expect(createObjectURL).toHaveBeenCalledWith(csvBlob);
    expect(revokeObjectURL).toHaveBeenCalledWith("blob:insights");
  });

  it("disables empty insight exports and reports generation failures", async () => {
    const baseFetch = defaultFetchMock();
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const path = String(input);
      if (path.startsWith("/api/v1/insights?")) {
        return response({ columns: insightColumns, rows: [{ paper_id: "W123" }] });
      }
      if (path.startsWith("/api/v1/insights/export.csv?")) {
        return response({ detail: "CSV generation failed." }, false, 500, "Server Error");
      }
      return baseFetch(input, init);
    });
    renderApp(fetchMock, "/insights");

    await screen.findByText("W123");
    fireEvent.click(screen.getByRole("button", { name: "Download CSV" }));
    fireEvent.click(screen.getByRole("button", { name: "Clear" }));
    expect(screen.getByRole("button", { name: "Download CSV" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "Select all" }));
    fireEvent.click(screen.getByRole("button", { name: "Download CSV" }));

    expect(await screen.findByText("CSV generation failed.")).toBeInTheDocument();
  });

  it("browses the verified UM dataset catalog and opens raw details", async () => {
    renderApp(defaultFetchMock(), "/datasets");

    expect(await screen.findByText("Catalog verified")).toBeInTheDocument();
    expect(screen.getByText("Showing 1–1 of 51")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: /Maastricht Health Dataset/ }));

    expect(await screen.findByText("Raw metadata")).toBeInTheDocument();
    expect(screen.getByText(/openalex_pure_export/)).toBeInTheDocument();
  });

  it("paginates through the complete UM dataset catalog", async () => {
    renderApp(defaultFetchMock(), "/datasets");

    await screen.findByText("Maastricht Health Dataset");
    fireEvent.click(screen.getByRole("button", { name: "Next" }));

    expect(await screen.findByText("Final UM Dataset")).toBeInTheDocument();
    expect(screen.getByText("Showing 51–51 of 51")).toBeInTheDocument();
  });

  it("applies UM dataset search and year filters", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/datasets");

    await screen.findByText("Maastricht Health Dataset");
    fireEvent.change(screen.getByLabelText("Filter by year"), {
      target: { value: "2024" },
    });
    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/um-datasets?year=2024&offset=0&limit=50",
        expect.objectContaining({ method: "GET" }),
      );
    });
    fireEvent.change(screen.getByLabelText("Search UM datasets"), {
      target: { value: "health data" },
    });
    fireEvent.click(screen.getByRole("button", { name: "Search" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/um-datasets?q=health+data&year=2024&offset=0&limit=50",
        expect.objectContaining({ method: "GET" }),
      );
    });
  });

  it("shows changed fields when UM catalog verification finds mismatches", async () => {
    const baseFetch = defaultFetchMock();
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/api/v1/um-datasets/verification") {
        return response({
          status: "mismatch",
          source_path: "data/um_dataset",
          checked_at: "2026-07-21T00:00:00Z",
          source_count: 51,
          stored_count: 51,
          verified_count: 50,
          issues: [
            {
              um_dataset_id: "W123",
              title: "Maastricht Health Dataset",
              status: "changed",
              changed_fields: ["title", "raw"],
            },
          ],
          warnings: [],
          metrics: {},
          message: null,
        });
      }
      return baseFetch(input, init);
    });
    renderApp(fetchMock, "/datasets");

    expect(await screen.findByText(/Stored catalog differs/)).toBeInTheDocument();
    expect(screen.getByText("title, raw")).toBeInTheDocument();
  });

  it("offers to import the authoritative catalog when storage is empty", async () => {
    const baseFetch = defaultFetchMock();
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/api/v1/um-datasets/verification") {
        return response({
          status: "not_imported",
          source_path: "data/um_dataset",
          checked_at: "2026-08-14T00:00:00Z",
          source_count: 2748,
          stored_count: 0,
          verified_count: 0,
          issues: [],
          warnings: [],
          metrics: {},
          message: "The authoritative catalog is available but has not been imported into the database.",
        });
      }
      if (String(input) === "/api/v1/um-datasets/import") {
        return response({ count: 2748, warnings: [], metrics: {} });
      }
      return baseFetch(input, init);
    });
    renderApp(fetchMock, "/datasets");

    expect(await screen.findByText("Catalog setup required")).toBeInTheDocument();
    expect(screen.queryByText("Stored catalog differs from the authoritative source")).not.toBeInTheDocument();
    expect(screen.queryByRole("textbox", { name: "Search UM datasets" })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Import authoritative catalog" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/um-datasets/import",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ path: "data/um_dataset" }),
        }),
      );
    });
  });

  it("keeps stored UM datasets browsable when verification is unavailable", async () => {
    const baseFetch = defaultFetchMock();
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      if (String(input) === "/api/v1/um-datasets/verification") {
        return response({
          status: "unavailable",
          source_path: "data/um_dataset",
          checked_at: "2026-07-21T00:00:00Z",
          source_count: null,
          stored_count: 51,
          verified_count: 0,
          issues: [],
          warnings: [],
          metrics: {},
          message: "Authoritative export is missing",
        });
      }
      return baseFetch(input, init);
    });
    renderApp(fetchMock, "/datasets");

    expect(await screen.findByText("Catalog verification unavailable")).toBeInTheDocument();
    expect(screen.getByText("Maastricht Health Dataset")).toBeInTheDocument();
  });

  it("submits high-throughput launch strategy from the toggle", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/launch");

    fireEvent.click(await screen.findByLabelText("High-throughput mode"));
    fireEvent.click(screen.getByRole("button", { name: "Run pipeline" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/runs",
        expect.objectContaining({
          method: "POST",
          body: expect.stringContaining('"strategy":"high_throughput"'),
        }),
      );
    });
  });

  it("prepares the adaptive funnel in one click and opens the live workspace", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/launch");

    const runButton = await screen.findByRole("button", { name: "Run pipeline" });
    await waitFor(() => expect(runButton).toBeEnabled());
    fireEvent.click(runButton);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/discovery/preview",
        expect.objectContaining({ method: "POST" }),
      );
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/runs",
        expect.objectContaining({
          method: "POST",
          body: expect.stringContaining('"preview_id":"preview-123"'),
        }),
      );
    });

    await waitFor(() => expect(window.location.pathname).toBe("/runs/40"));
    expect(await screen.findByText("Run graph")).toBeInTheDocument();
    expect(screen.getAllByText("running").length).toBeGreaterThan(0);
  });

  it("selects a reproducible random sample strategy", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/launch");

    fireEvent.click(await screen.findByRole("button", { name: "Select Random sample" }));
    expect(screen.getByLabelText("Random sample size")).toBeInTheDocument();
    expect(screen.getByLabelText("Sample seed")).toBeInTheDocument();
    expect(screen.queryByLabelText("Optional focus")).not.toBeInTheDocument();
    fireEvent.change(screen.getByLabelText("Random sample size"), { target: { value: "120" } });
    fireEvent.change(screen.getByLabelText("Sample seed"), { target: { value: "42" } });
    fireEvent.click(screen.getByRole("button", { name: "Preview results" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/discovery/preview",
        expect.objectContaining({
          method: "POST",
          body: expect.stringContaining('"mode":"random"'),
        }),
      );
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/discovery/preview",
        expect.objectContaining({ body: expect.stringContaining('"random_seed":42') }),
      );
    });
    expect(await screen.findAllByText("Not scored")).not.toHaveLength(0);
    expect(screen.getByText(/Random inclusion is not evidence of dataset use/)).toBeInTheDocument();
  });

  it("previews the adaptive funnel and launches only reviewed PDF-ready candidates", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock, "/launch");

    fireEvent.change(await screen.findByLabelText("Candidate pool cap"), {
      target: { value: "120" },
    });
    const previewButton = await screen.findByRole("button", { name: "Preview results" });
    await waitFor(() => expect(previewButton).toBeEnabled());
    fireEvent.click(previewButton);

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/discovery/preview",
        expect.objectContaining({ body: expect.stringContaining('"discovery_limit":120') }),
      );
    });

    expect(await screen.findByText("Secondary analysis of a Maastricht longitudinal cohort")).toBeInTheDocument();
    expect(screen.getByText("Potential reuse without downloadable full text")).toBeInTheDocument();
    expect(screen.getByText("Watchlist")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Run pipeline" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/runs",
        expect.objectContaining({
          method: "POST",
          body: expect.stringContaining('"preview_id":"preview-123"'),
        }),
      );
    });

    await waitFor(() => expect(window.location.pathname).toBe("/runs/40"));
    expect(await screen.findByText("Run graph")).toBeInTheDocument();
    expect(screen.getAllByText("running").length).toBeGreaterThan(0);
  });

  it("renders the workspace graph and selected stage inspector", async () => {
    const { container } = renderApp(defaultFetchMock(), "/runs/39");

    expect(await screen.findByText("Run graph")).toBeInTheDocument();
    expect(screen.getAllByText("High-throughput").length).toBeGreaterThan(0);
    expect(screen.getByText("Parallel: 2 stages active")).toBeInTheDocument();
    expect(container.querySelector('[data-edge="discover-download_pdf"]')).toBeInTheDocument();
    expect(container.querySelector("#pipelineGlow")).toHaveAttribute(
      "filterUnits",
      "userSpaceOnUse",
    );
    expect(screen.getAllByText("Discover").length).toBeGreaterThan(0);
    const downloadNode = screen.getByLabelText("Inspect Download Pdf status and filter events");
    expect(downloadNode).toBeInTheDocument();
    fireEvent.click(downloadNode);
    expect(screen.getByText("Download PDFs.")).toBeInTheDocument();
  });
});
