export type HealthResponse = {
  database_ready: boolean;
  redis_ready: boolean;
  worker_ready: boolean;
  grobid_ready: boolean;
  details: Record<string, unknown>;
};

export type StageInfo = {
  name: string;
  description: string;
};

export type StageRunSummary = {
  id: number;
  stage: string;
  status: string;
  attempt_count: number;
  task_id: string | null;
  error: string | null;
  metrics: Record<string, unknown>;
  started_at: string | null;
  finished_at: string | null;
  created_at: string | null;
  updated_at: string | null;
};

export type PipelineRunSummary = {
  id: number;
  run_key: string;
  query: string | null;
  status: string;
  config: Record<string, unknown>;
  celery_task_id: string | null;
  error: string | null;
  created_at: string | null;
  updated_at: string | null;
  finished_at: string | null;
  stages: StageRunSummary[];
};

export type PipelineRunEvent = {
  id: number;
  pipeline_run_id: number;
  stage: string | null;
  level: string;
  message: string;
  payload: Record<string, unknown>;
  created_at: string | null;
};

export type RunCreateRequest = {
  query: string;
  limit: number;
  fields_of_study?: string | null;
  open_access_only: boolean;
  overwrite: boolean;
  um_datasets_path?: string | null;
  output_path: string;
};

export type RunCreateResponse = {
  pipeline_run_id: number;
  task_id: string | null;
  status: string;
};

export type InsightsResponse = {
  rows: Record<string, unknown>[];
};

export type ResetRequest = {
  confirm: string;
  force: boolean;
};

export type ResetResponse = {
  status: string;
  active_runs: number;
  truncated_tables: string[];
  deleted_paths: string[];
  recreated_directories: string[];
};

type RequestOptions = {
  method?: "GET" | "POST";
  body?: unknown;
};

export class ApiError extends Error {
  status: number;
  detail: unknown;

  constructor(status: number, detail: unknown) {
    super(typeof detail === "string" ? detail : `API request failed with status ${status}`);
    this.status = status;
    this.detail = detail;
  }
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const response = await fetch(path, {
    method: options.method ?? "GET",
    headers: options.body ? { "Content-Type": "application/json" } : undefined,
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  if (!response.ok) {
    let detail: unknown = response.statusText;
    try {
      detail = (await response.json()).detail ?? detail;
    } catch {
      detail = response.statusText;
    }
    throw new ApiError(response.status, detail);
  }

  return response.json() as Promise<T>;
}

export const api = {
  health: () => request<HealthResponse>("/api/v1/health"),
  stages: async () => {
    const response = await request<{ stages: StageInfo[] }>("/api/v1/stages");
    return response.stages;
  },
  runs: async (limit = 25) => {
    const params = new URLSearchParams({ limit: String(limit) });
    const response = await request<{ runs: PipelineRunSummary[] }>(`/api/v1/runs?${params}`);
    return response.runs;
  },
  run: (runId: number) => request<PipelineRunSummary>(`/api/v1/runs/${runId}`),
  runEvents: async (runId: number, limit = 200) => {
    const params = new URLSearchParams({ limit: String(limit) });
    const response = await request<{ events: PipelineRunEvent[] }>(
      `/api/v1/runs/${runId}/events?${params}`,
    );
    return response.events;
  },
  createRun: (body: RunCreateRequest) =>
    request<RunCreateResponse>("/api/v1/runs", { method: "POST", body }),
  insights: (limit = 100) => {
    const params = new URLSearchParams({ limit: String(limit) });
    return request<InsightsResponse>(`/api/v1/insights?${params}`);
  },
  reset: (body: ResetRequest) =>
    request<ResetResponse>("/api/v1/admin/reset", { method: "POST", body }),
};

export function isActiveRun(status: string | undefined) {
  return status === "queued" || status === "running" || status === "started";
}
