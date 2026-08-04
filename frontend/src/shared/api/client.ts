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
  topic_ids?: string[] | null;
  keyword_terms?: string[] | null;
  mesh_terms?: string[] | null;
  from_year?: number | null;
  to_year?: number | null;
  use_um_profile?: boolean;
  open_access_only: boolean;
  overwrite: boolean;
  um_datasets_path?: string | null;
  output_path: string;
  strategy?: "standard" | "high_throughput";
};

export type RunCreateResponse = {
  pipeline_run_id: number;
  task_id: string | null;
  status: string;
};

export type InsightsResponse = {
  rows: Record<string, unknown>[];
};

export type UMDatasetSummary = {
  um_dataset_id: string;
  title: string;
  aliases: string[];
  creators: string[];
  doi: string | null;
  url: string | null;
  year: number | null;
  repository: string | null;
  keywords: string[];
  created_at: string | null;
  updated_at: string | null;
};

export type UMDatasetDetail = UMDatasetSummary & {
  raw: Record<string, unknown>;
};

export type UMDatasetListResponse = {
  items: UMDatasetSummary[];
  total: number;
  offset: number;
  limit: number;
  repositories: string[];
  years: number[];
};

export type UMDatasetVerificationIssue = {
  um_dataset_id: string;
  title: string;
  status: "missing" | "unexpected" | "changed";
  changed_fields: string[];
};

export type UMDatasetVerificationResponse = {
  status: "verified" | "mismatch" | "unavailable";
  source_path: string;
  checked_at: string;
  source_count: number | null;
  stored_count: number;
  verified_count: number;
  issues: UMDatasetVerificationIssue[];
  warnings: string[];
  metrics: Record<string, unknown>;
  message: string | null;
};

export type UMDatasetListParams = {
  q?: string;
  repository?: string;
  year?: number;
  offset?: number;
  limit?: number;
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
  umDatasets: (filters: UMDatasetListParams = {}) => {
    const params = new URLSearchParams();
    if (filters.q) params.set("q", filters.q);
    if (filters.repository) params.set("repository", filters.repository);
    if (filters.year !== undefined) params.set("year", String(filters.year));
    params.set("offset", String(filters.offset ?? 0));
    params.set("limit", String(filters.limit ?? 50));
    return request<UMDatasetListResponse>(`/api/v1/um-datasets?${params}`);
  },
  umDataset: (umDatasetId: string) =>
    request<UMDatasetDetail>(`/api/v1/um-datasets/${encodeURIComponent(umDatasetId)}`),
  verifyUmDatasets: () =>
    request<UMDatasetVerificationResponse>("/api/v1/um-datasets/verification"),
  reset: (body: ResetRequest) =>
    request<ResetResponse>("/api/v1/admin/reset", { method: "POST", body }),
};

export function isActiveRun(status: string | undefined) {
  return status === "queued" || status === "running" || status === "started";
}
