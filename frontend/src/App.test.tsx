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
    if (path === "/api/v1/runs/39") {
      return response({
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
      });
    }
    if (path.startsWith("/api/v1/runs/39/events")) {
      return response({
        events: [
          {
            id: 1,
            pipeline_run_id: 39,
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
      return response({ rows: [] });
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

    expect(await screen.findByText("Start a dataset mention run")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("link", { name: /^Runs/ }));

    expect(await screen.findByText("Open a run to inspect the workspace, events, and stage metrics.")).toBeInTheDocument();
    expect(screen.getByText("Maastricht dataset reuse")).toBeInTheDocument();
    expect(screen.getByText("High-throughput")).toBeInTheDocument();
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
    fireEvent.click(screen.getByRole("button", { name: /Start run/ }));

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
