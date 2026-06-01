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
            config: {},
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
        config: {},
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
    if (path === "/api/v1/admin/reset" && init?.method === "POST") {
      return resetResponse;
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
  });

  it("renders the workspace graph and selected stage inspector", async () => {
    const { container } = renderApp(defaultFetchMock(), "/runs/39");

    expect(await screen.findByText("Run graph")).toBeInTheDocument();
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
