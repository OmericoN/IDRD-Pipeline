import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { cleanup, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { afterEach, describe, expect, it, vi } from "vitest";

import App from "./App";

function renderApp(fetchMock: ReturnType<typeof vi.fn>) {
  vi.stubGlobal("fetch", fetchMock);
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  });

  return render(
    <QueryClientProvider client={queryClient}>
      <App />
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
        stages: [{ name: "discover", description: "Find publication metadata." }],
      });
    }
    if (path.startsWith("/api/v1/runs?")) {
      return response({ runs: [] });
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
    renderApp(fetchMock);

    fireEvent.click(screen.getByLabelText("Admin settings"));
    fireEvent.click(screen.getByText("Danger zone"));

    const resetButton = screen.getByRole("button", { name: "Reset everything" });
    expect(resetButton).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText("RESET IDRD"), { target: { value: "RESET" } });
    expect(resetButton).toBeDisabled();

    fireEvent.change(screen.getByPlaceholderText("RESET IDRD"), { target: { value: "RESET IDRD" } });
    fireEvent.click(screen.getByLabelText("I understand this action cannot be undone from the app."));
    expect(resetButton).toBeEnabled();
  });

  it("submits reset with force setting and shows the success result", async () => {
    const fetchMock = defaultFetchMock();
    renderApp(fetchMock);

    fireEvent.click(screen.getByLabelText("Admin settings"));
    fireEvent.click(screen.getByText("Danger zone"));
    fireEvent.change(screen.getByPlaceholderText("RESET IDRD"), { target: { value: "RESET IDRD" } });
    fireEvent.click(screen.getByLabelText("I understand this action cannot be undone from the app."));
    fireEvent.click(screen.getByLabelText("Force reset active runs"));
    fireEvent.click(screen.getByRole("button", { name: "Reset everything" }));

    await waitFor(() => {
      expect(fetchMock).toHaveBeenCalledWith(
        "/api/v1/admin/reset",
        expect.objectContaining({
          method: "POST",
          body: JSON.stringify({ confirm: "RESET IDRD", force: true }),
        }),
      );
    });
    expect(await screen.findByText(/Reset complete/)).toBeInTheDocument();
  });

  it("explains active-run conflicts without enabling force automatically", async () => {
    const fetchMock = defaultFetchMock(
      response({ detail: "1 pipeline run(s) are still active." }, false, 409, "Conflict"),
    );
    renderApp(fetchMock);

    fireEvent.click(screen.getByLabelText("Admin settings"));
    fireEvent.click(screen.getByText("Danger zone"));
    fireEvent.change(screen.getByPlaceholderText("RESET IDRD"), { target: { value: "RESET IDRD" } });
    fireEvent.click(screen.getByLabelText("I understand this action cannot be undone from the app."));
    fireEvent.click(screen.getByRole("button", { name: "Reset everything" }));

    expect(await screen.findByText(/Retry only if you are sure by enabling force reset/)).toBeInTheDocument();
    expect(screen.getByLabelText("Force reset active runs")).not.toBeChecked();
  });
});
