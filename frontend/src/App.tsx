import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  ChevronDown,
  Circle,
  Clock3,
  Copy,
  Database,
  FileText,
  Loader2,
  Play,
  RefreshCw,
  Server,
  Settings,
  ShieldAlert,
  TerminalSquare,
  Trash2,
  X,
  XCircle,
} from "lucide-react";
import { useMemo, useState, type FormEvent, type ReactNode, type SetStateAction } from "react";

import { Badge } from "./components/ui/badge";
import { Button } from "./components/ui/button";
import { Input } from "./components/ui/input";
import { Label } from "./components/ui/label";
import { Progress } from "./components/ui/progress";
import { Switch } from "./components/ui/switch";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./components/ui/table";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./components/ui/tabs";
import {
  ApiError,
  api,
  isActiveRun,
  type PipelineRunEvent,
  type PipelineRunSummary,
  type ResetResponse,
} from "./lib/api";
import { cn } from "./lib/utils";
import { formatStageName, mergeStages, stageProgress, statusTone, type VisualStage } from "./lib/pipeline";

const DEFAULT_OUTPUT = "storage/exports/insights.csv";
const DEFAULT_UM_DATASETS = "data/um_datasets.csv";
const RESET_CONFIRMATION = "RESET IDRD";

function App() {
  const queryClient = useQueryClient();
  const [selectedRunId, setSelectedRunId] = useState<number | null>(null);
  const [eventStage, setEventStage] = useState("all");
  const [tab, setTab] = useState("logs");
  const [adminOpen, setAdminOpen] = useState(false);
  const [dangerOpen, setDangerOpen] = useState(false);
  const [resetConfirm, setResetConfirm] = useState("");
  const [resetAcknowledged, setResetAcknowledged] = useState(false);
  const [resetForce, setResetForce] = useState(false);
  const [resetResult, setResetResult] = useState<ResetResponse | null>(null);
  const [form, setForm] = useState({
    query: "Maastricht dataset reuse",
    limit: 25,
    umDatasetsPath: DEFAULT_UM_DATASETS,
    outputPath: DEFAULT_OUTPUT,
    openAccessOnly: true,
    overwrite: false,
  });

  const healthQuery = useQuery({
    queryKey: ["health"],
    queryFn: api.health,
    refetchInterval: 5000,
  });
  const stagesQuery = useQuery({ queryKey: ["stages"], queryFn: api.stages });
  const runsQuery = useQuery({
    queryKey: ["runs"],
    queryFn: () => api.runs(25),
    refetchInterval: 5000,
  });
  const selectedRunQuery = useQuery({
    queryKey: ["run", selectedRunId],
    queryFn: () => api.run(selectedRunId!),
    enabled: selectedRunId !== null,
    refetchInterval: (query) => (isActiveRun(query.state.data?.status) ? 2500 : false),
  });
  const eventsQuery = useQuery({
    queryKey: ["run-events", selectedRunId],
    queryFn: () => api.runEvents(selectedRunId!, 200),
    enabled: selectedRunId !== null,
    refetchInterval: () => {
      const run = queryClient.getQueryData<PipelineRunSummary>(["run", selectedRunId]);
      return isActiveRun(run?.status) ? 2500 : false;
    },
  });
  const insightsQuery = useQuery({
    queryKey: ["insights"],
    queryFn: () => api.insights(100),
    refetchInterval: selectedRunQuery.data && isActiveRun(selectedRunQuery.data.status) ? 10000 : false,
  });

  const createRunMutation = useMutation({
    mutationFn: api.createRun,
    onSuccess: async (created) => {
      setSelectedRunId(created.pipeline_run_id);
      setEventStage("all");
      setTab("logs");
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["runs"] }),
        queryClient.invalidateQueries({ queryKey: ["run", created.pipeline_run_id] }),
        queryClient.invalidateQueries({ queryKey: ["run-events", created.pipeline_run_id] }),
      ]);
    },
  });

  const resetMutation = useMutation({
    mutationFn: api.reset,
    onSuccess: async (result) => {
      setResetResult(result);
      setSelectedRunId(null);
      setEventStage("all");
      setTab("logs");
      setResetConfirm("");
      setResetAcknowledged(false);
      setResetForce(false);
      await Promise.all([
        queryClient.invalidateQueries({ queryKey: ["health"] }),
        queryClient.invalidateQueries({ queryKey: ["runs"] }),
        queryClient.invalidateQueries({ queryKey: ["run"] }),
        queryClient.invalidateQueries({ queryKey: ["run-events"] }),
        queryClient.invalidateQueries({ queryKey: ["insights"] }),
      ]);
    },
  });

  const runs = runsQuery.data ?? [];
  const selectedRun = selectedRunQuery.data ?? runs.find((run) => run.id === selectedRunId);
  const visualStages = useMemo(
    () => mergeStages(stagesQuery.data ?? [], selectedRun),
    [stagesQuery.data, selectedRun],
  );
  const filteredEvents = useMemo(() => {
    const events = eventsQuery.data ?? [];
    if (eventStage === "all") {
      return events;
    }
    return events.filter((event) => event.stage === eventStage);
  }, [eventStage, eventsQuery.data]);
  const stageFilters = useMemo(
    () => [
      "all",
      ...Array.from(
        new Set(
          (eventsQuery.data ?? [])
            .map((event) => event.stage)
            .filter((stage): stage is string => Boolean(stage)),
        ),
      ),
    ],
    [eventsQuery.data],
  );

  function submitRun(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    createRunMutation.mutate({
      query: form.query.trim(),
      limit: form.limit,
      open_access_only: form.openAccessOnly,
      overwrite: form.overwrite,
      um_datasets_path: form.umDatasetsPath.trim() || null,
      output_path: form.outputPath.trim() || DEFAULT_OUTPUT,
    });
  }

  function refreshAll() {
    void healthQuery.refetch();
    void runsQuery.refetch();
    void insightsQuery.refetch();
    if (selectedRunId) {
      void selectedRunQuery.refetch();
      void eventsQuery.refetch();
    }
  }

  const canReset = resetConfirm === RESET_CONFIRMATION && resetAcknowledged;

  return (
    <main className="min-h-screen bg-[var(--background)] text-[var(--foreground)]">
      <header className="sticky top-0 z-20 border-b border-[var(--border)] bg-[rgba(250,250,248,0.94)] backdrop-blur">
        <div className="mx-auto max-w-7xl px-4 py-4 sm:px-6 lg:px-8">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div className="min-w-0">
              <p className="text-xs font-semibold uppercase tracking-[0.18em] text-[var(--muted-foreground)]">
                IDRD Pipeline
              </p>
              <h1 className="mt-1 text-2xl font-semibold text-[var(--foreground)]">
                Dataset mention workflow
              </h1>
            </div>
            <div className="flex flex-col gap-3 sm:flex-row sm:items-center">
              <HealthStrip health={healthQuery.data} isLoading={healthQuery.isLoading} />
              <div className="flex items-center gap-2">
                <Button type="button" variant="outline" onClick={refreshAll}>
                  <RefreshCw className="h-4 w-4" />
                  Refresh
                </Button>
                <Button
                  type="button"
                  variant="ghost"
                  size="icon"
                  aria-label="Admin settings"
                  title="Admin settings"
                  onClick={() => setAdminOpen(true)}
                >
                  <Settings className="h-4 w-4" />
                </Button>
              </div>
            </div>
          </div>
        </div>
      </header>

      <div className="mx-auto grid max-w-7xl gap-5 px-4 py-5 sm:px-6 lg:grid-cols-[380px_minmax(0,1fr)] lg:px-8">
        <aside className="space-y-4 lg:sticky lg:top-[112px] lg:self-start">
          <RunLauncher
            form={form}
            isPending={createRunMutation.isPending}
            error={createRunMutation.error}
            onSubmit={submitRun}
            onChange={setForm}
          />
          <RecentRuns
            runs={runs}
            selectedRunId={selectedRunId}
            isLoading={runsQuery.isLoading}
            onSelect={(runId) => {
              setSelectedRunId(runId);
              setEventStage("all");
            }}
          />
        </aside>

        <section className="min-w-0 space-y-5">
          <RunOverview run={selectedRun} visualStages={visualStages} />
          <StagePanel stages={visualStages} />
          <ActivityPanel
            tab={tab}
            setTab={setTab}
            stageFilters={stageFilters}
            eventStage={eventStage}
            setEventStage={setEventStage}
            events={filteredEvents}
            eventsLoading={eventsQuery.isLoading && selectedRunId !== null}
            hasRun={selectedRunId !== null}
            insightRows={insightsQuery.data?.rows ?? []}
            insightsLoading={insightsQuery.isLoading}
          />
        </section>
      </div>

      {adminOpen ? (
        <AdminDialog
          canReset={canReset}
          dangerOpen={dangerOpen}
          resetAcknowledged={resetAcknowledged}
          resetConfirm={resetConfirm}
          resetError={resetMutation.error}
          resetForce={resetForce}
          resetPending={resetMutation.isPending}
          resetResult={resetResult}
          onClose={() => setAdminOpen(false)}
          onDangerOpenChange={setDangerOpen}
          onReset={() => resetMutation.mutate({ confirm: resetConfirm, force: resetForce })}
          onResetAcknowledgedChange={setResetAcknowledged}
          onResetConfirmChange={(value) => {
            setResetConfirm(value);
            setResetResult(null);
            resetMutation.reset();
          }}
          onResetForceChange={setResetForce}
        />
      ) : null}
    </main>
  );
}

function HealthStrip({
  health,
  isLoading,
}: {
  health: Awaited<ReturnType<typeof api.health>> | undefined;
  isLoading: boolean;
}) {
  const checks = [
    { label: "Database", ready: health?.database_ready, icon: Database },
    { label: "Redis", ready: health?.redis_ready, icon: Activity },
    { label: "Worker", ready: health?.worker_ready, icon: Server },
    { label: "GROBID", ready: health?.grobid_ready, icon: FileText },
  ];

  return (
    <div className="grid min-w-0 grid-cols-1 gap-2 sm:grid-cols-2 xl:grid-cols-4">
      {checks.map((check) => {
        const Icon = check.icon;
        const ready = Boolean(check.ready);
        return (
          <div
            key={check.label}
            className="flex min-w-[128px] items-center justify-between gap-3 rounded-md border border-[var(--border)] bg-[var(--surface)] px-3 py-2 shadow-[var(--shadow-subtle)]"
          >
            <div className="flex min-w-0 items-center gap-2">
              <Icon className="h-4 w-4 shrink-0 text-[var(--muted-foreground)]" />
              <span className="truncate text-sm font-medium text-[var(--foreground)]">{check.label}</span>
            </div>
            <Badge variant={isLoading ? "muted" : ready ? "success" : "error"}>
              {isLoading ? "checking" : ready ? "ready" : "offline"}
            </Badge>
          </div>
        );
      })}
    </div>
  );
}

function RunLauncher({
  form,
  isPending,
  error,
  onSubmit,
  onChange,
}: {
  form: {
    query: string;
    limit: number;
    umDatasetsPath: string;
    outputPath: string;
    openAccessOnly: boolean;
    overwrite: boolean;
  };
  isPending: boolean;
  error: Error | null;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  onChange: (updater: SetStateAction<typeof form>) => void;
}) {
  return (
    <section className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 shadow-[var(--shadow-panel)]">
      <PanelHeader
        title="Run pipeline"
        description="Queue a full workflow through FastAPI."
        icon={<Play className="h-4 w-4" />}
      />
      <form className="mt-4 space-y-4" onSubmit={onSubmit}>
        <Field label="Search query">
          <Input
            value={form.query}
            onChange={(event) => onChange((current) => ({ ...current, query: event.target.value }))}
            placeholder="Maastricht dataset reuse"
            required
          />
        </Field>
        <div className="grid gap-3 sm:grid-cols-[120px_minmax(0,1fr)]">
          <Field label="Limit">
            <Input
              type="number"
              min={1}
              max={1000}
              value={form.limit}
              onChange={(event) => onChange((current) => ({ ...current, limit: Number(event.target.value) }))}
            />
          </Field>
          <Field label="Output CSV">
            <Input
              value={form.outputPath}
              onChange={(event) => onChange((current) => ({ ...current, outputPath: event.target.value }))}
            />
          </Field>
        </div>
        <Field label="UM dataset metadata">
          <Input
            value={form.umDatasetsPath}
            onChange={(event) => onChange((current) => ({ ...current, umDatasetsPath: event.target.value }))}
          />
        </Field>
        <div className="grid gap-2 rounded-md border border-[var(--border)] bg-[var(--muted)] p-3 sm:grid-cols-2">
          <ToggleRow
            label="Open access"
            checked={form.openAccessOnly}
            onChange={(checked) => onChange((current) => ({ ...current, openAccessOnly: checked }))}
          />
          <ToggleRow
            label="Overwrite"
            checked={form.overwrite}
            onChange={(checked) => onChange((current) => ({ ...current, overwrite: checked }))}
          />
        </div>
        {error ? <Alert tone="error">{error.message}</Alert> : null}
        <Button className="w-full" type="submit" disabled={isPending || !form.query.trim()}>
          {isPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Play className="h-4 w-4" />}
          {isPending ? "Starting" : "Start run"}
        </Button>
      </form>
    </section>
  );
}

function RecentRuns({
  runs,
  selectedRunId,
  isLoading,
  onSelect,
}: {
  runs: PipelineRunSummary[];
  selectedRunId: number | null;
  isLoading: boolean;
  onSelect: (runId: number) => void;
}) {
  return (
    <section className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 shadow-[var(--shadow-panel)]">
      <div className="mb-3 flex items-center justify-between">
        <h2 className="text-sm font-semibold text-[var(--foreground)]">Recent runs</h2>
        <Badge variant="muted">{runs.length}</Badge>
      </div>
      <div className="max-h-[520px] space-y-2 overflow-y-auto pr-1">
        {isLoading ? <EmptyState text="Loading runs..." /> : null}
        {!isLoading && !runs.length ? <EmptyState text="No pipeline runs yet." /> : null}
        {runs.map((run) => (
          <button
            key={run.id}
            type="button"
            className={cn(
              "w-full rounded-md border p-3 text-left transition-colors hover:bg-[var(--surface-hover)]",
              selectedRunId === run.id
                ? "border-[var(--accent)] bg-[var(--accent-soft)]"
                : "border-[var(--border)] bg-[var(--surface)]",
            )}
            onClick={() => onSelect(run.id)}
          >
            <div className="flex items-center justify-between gap-2">
              <span className="truncate text-sm font-semibold text-[var(--foreground)]">
                {run.query || `Run ${run.id}`}
              </span>
              <Badge variant={statusTone(run.status)}>{run.status}</Badge>
            </div>
            <p className="mt-2 text-xs text-[var(--muted-foreground)]">
              #{run.id} · {formatDate(run.created_at)}
            </p>
          </button>
        ))}
      </div>
    </section>
  );
}

function RunOverview({
  run,
  visualStages,
}: {
  run: PipelineRunSummary | undefined;
  visualStages: VisualStage[];
}) {
  const progress = stageProgress(visualStages);

  return (
    <section className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 shadow-[var(--shadow-panel)]">
      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr)_360px] xl:items-center">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="truncate text-xl font-semibold text-[var(--foreground)]">
              {run ? run.query || `Run ${run.id}` : "No run selected"}
            </h2>
            {run ? <Badge variant={statusTone(run.status)}>{run.status}</Badge> : null}
          </div>
          <p className="mt-2 text-sm text-[var(--muted-foreground)]">
            {run
              ? `Run #${run.id} · started ${formatDate(run.created_at)}`
              : "Start a run or choose one from the sidebar to inspect progress."}
          </p>
          {run?.error ? <Alert tone="error">{run.error}</Alert> : null}
        </div>
        <div className="grid gap-3">
          <div className="flex items-center justify-between gap-3">
            <span className="text-xs font-medium text-[var(--muted-foreground)]">Stage progress</span>
            <span className="text-xs font-semibold text-[var(--foreground)]">{progress}%</span>
          </div>
          <Progress value={progress} />
          <div className="grid grid-cols-3 gap-2 text-center">
            <Metric label="Done" value={visualStages.filter((stage) => stage.status === "successful").length} />
            <Metric label="Skipped" value={visualStages.filter((stage) => stage.status === "skipped").length} />
            <Metric label="Errors" value={visualStages.filter((stage) => stage.status === "failed").length} />
          </div>
        </div>
      </div>
    </section>
  );
}

function StagePanel({ stages }: { stages: VisualStage[] }) {
  return (
    <section className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 shadow-[var(--shadow-panel)]">
      <PanelHeader
        title="Pipeline stages"
        description="Canonical backend order with live run state."
        icon={<Activity className="h-4 w-4" />}
      />
      <div className="mt-4">
        <StageTimeline stages={stages} />
      </div>
    </section>
  );
}

function StageTimeline({ stages }: { stages: VisualStage[] }) {
  if (!stages.length) {
    return <EmptyState text="Stage metadata is not available yet." />;
  }

  return (
    <div className="divide-y divide-[var(--border)] overflow-hidden rounded-md border border-[var(--border)]">
      {stages.map((stage) => (
        <div
          key={stage.name}
          className={cn(
            "grid gap-3 bg-[var(--surface)] px-3 py-3 md:grid-cols-[220px_minmax(0,1fr)_auto]",
            stage.status === "running" && "bg-[var(--accent-soft)]",
            stage.status === "failed" && "bg-red-50",
          )}
        >
          <div className="flex items-center gap-3">
            <StatusIcon status={stage.status} />
            <div className="min-w-0">
              <p className="truncate text-sm font-semibold capitalize text-[var(--foreground)]">
                {formatStageName(stage.name)}
              </p>
              <p className="text-xs text-[var(--muted-foreground)]">
                {stage.run ? formatDate(stage.run.updated_at) : "waiting"}
              </p>
            </div>
          </div>
          <p className="text-sm leading-6 text-[var(--muted-foreground)]">{stage.description}</p>
          <div className="flex flex-wrap items-center gap-2 md:justify-end">
            <Badge variant={statusTone(stage.status)}>{stage.status}</Badge>
            {stage.run?.metrics ? <MetricBadge metrics={stage.run.metrics} /> : null}
          </div>
          {stage.run?.error ? (
            <div className="md:col-span-3">
              <Alert tone="error">{stage.run.error}</Alert>
            </div>
          ) : null}
        </div>
      ))}
    </div>
  );
}

function ActivityPanel({
  tab,
  setTab,
  stageFilters,
  eventStage,
  setEventStage,
  events,
  eventsLoading,
  hasRun,
  insightRows,
  insightsLoading,
}: {
  tab: string;
  setTab: (value: string) => void;
  stageFilters: string[];
  eventStage: string;
  setEventStage: (value: string) => void;
  events: PipelineRunEvent[];
  eventsLoading: boolean;
  hasRun: boolean;
  insightRows: Record<string, unknown>[];
  insightsLoading: boolean;
}) {
  return (
    <section className="rounded-lg border border-[var(--border)] bg-[var(--surface)] p-4 shadow-[var(--shadow-panel)]">
      <Tabs value={tab} onValueChange={setTab}>
        <div className="flex flex-col justify-between gap-3 xl:flex-row xl:items-center">
          <TabsList>
            <TabsTrigger value="logs">
              <TerminalSquare className="mr-2 h-3.5 w-3.5" />
              Logs
            </TabsTrigger>
            <TabsTrigger value="insights">
              <FileText className="mr-2 h-3.5 w-3.5" />
              Insights
            </TabsTrigger>
          </TabsList>
          {tab === "logs" ? (
            <div className="flex min-w-0 flex-wrap gap-2">
              {stageFilters.map((stage) => (
                <Button
                  key={stage}
                  type="button"
                  variant={eventStage === stage ? "default" : "outline"}
                  size="sm"
                  onClick={() => setEventStage(stage)}
                >
                  {stage === "all" ? "All" : formatStageName(stage)}
                </Button>
              ))}
            </div>
          ) : null}
        </div>
        <TabsContent value="logs">
          <EventLog events={events} isLoading={eventsLoading} hasRun={hasRun} />
        </TabsContent>
        <TabsContent value="insights">
          <InsightsTable rows={insightRows} isLoading={insightsLoading} />
        </TabsContent>
      </Tabs>
    </section>
  );
}

function EventLog({
  events,
  isLoading,
  hasRun,
}: {
  events: PipelineRunEvent[];
  isLoading: boolean;
  hasRun: boolean;
}) {
  if (!hasRun) {
    return <EmptyState text="Select a run to see structured events." />;
  }
  if (isLoading) {
    return <EmptyState text="Loading events..." />;
  }
  if (!events.length) {
    return <EmptyState text="No events recorded for this filter." />;
  }

  return (
    <div className="max-h-[560px] space-y-2 overflow-y-auto pr-1">
      {events.map((event) => (
        <article
          key={event.id}
          className={cn(
            "rounded-md border px-3 py-3",
            event.level === "error"
              ? "border-red-200 bg-red-50"
              : event.level === "warning"
                ? "border-amber-200 bg-amber-50"
                : "border-[var(--border)] bg-[var(--muted)]",
          )}
        >
          <div className="flex flex-col justify-between gap-2 md:flex-row md:items-start">
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <Badge variant={statusTone(event.level)}>{event.level}</Badge>
                {event.stage ? <Badge variant="muted">{formatStageName(event.stage)}</Badge> : null}
                <span className="text-xs text-[var(--muted-foreground)]">{formatDate(event.created_at)}</span>
              </div>
              <p className="mt-2 text-sm font-medium text-[var(--foreground)]">{event.message}</p>
            </div>
            <Button
              type="button"
              variant="ghost"
              size="icon"
              title="Copy event payload"
              onClick={() => void navigator.clipboard.writeText(JSON.stringify(event, null, 2))}
            >
              <Copy className="h-4 w-4" />
            </Button>
          </div>
          {Object.keys(event.payload).length ? (
            <details className="mt-3 rounded-md border border-[var(--border)] bg-[var(--surface)]">
              <summary className="cursor-pointer px-3 py-2 text-xs font-medium text-[var(--muted-foreground)]">
                Payload
              </summary>
              <pre className="max-h-56 overflow-auto border-t border-[var(--border)] p-3 text-xs text-[var(--muted-foreground)]">
                {JSON.stringify(event.payload, null, 2)}
              </pre>
            </details>
          ) : null}
        </article>
      ))}
    </div>
  );
}

function InsightsTable({ rows, isLoading }: { rows: Record<string, unknown>[]; isLoading: boolean }) {
  if (isLoading) {
    return <EmptyState text="Loading insights..." />;
  }
  if (!rows.length) {
    return <EmptyState text="No insight rows available yet." />;
  }

  const columns = Object.keys(rows[0] ?? {}).slice(0, 8);
  return (
    <Table className="min-w-[860px]">
      <TableHeader>
        <TableRow>
          {columns.map((column) => (
            <TableHead key={column}>{formatStageName(column)}</TableHead>
          ))}
        </TableRow>
      </TableHeader>
      <TableBody>
        {rows.slice(0, 25).map((row, index) => (
          <TableRow key={index}>
            {columns.map((column) => (
              <TableCell key={column} className="max-w-[260px] truncate">
                {formatCell(row[column])}
              </TableCell>
            ))}
          </TableRow>
        ))}
      </TableBody>
    </Table>
  );
}

function AdminDialog({
  canReset,
  dangerOpen,
  resetAcknowledged,
  resetConfirm,
  resetError,
  resetForce,
  resetPending,
  resetResult,
  onClose,
  onDangerOpenChange,
  onReset,
  onResetAcknowledgedChange,
  onResetConfirmChange,
  onResetForceChange,
}: {
  canReset: boolean;
  dangerOpen: boolean;
  resetAcknowledged: boolean;
  resetConfirm: string;
  resetError: Error | null;
  resetForce: boolean;
  resetPending: boolean;
  resetResult: ResetResponse | null;
  onClose: () => void;
  onDangerOpenChange: (open: boolean) => void;
  onReset: () => void;
  onResetAcknowledgedChange: (checked: boolean) => void;
  onResetConfirmChange: (value: string) => void;
  onResetForceChange: (checked: boolean) => void;
}) {
  const resetErrorText =
    resetError instanceof ApiError && resetError.status === 409
      ? `${String(resetError.detail)} Retry only if you are sure by enabling force reset.`
      : resetError?.message;

  return (
    <div
      className="fixed inset-0 z-30 flex items-start justify-center bg-black/30 px-4 py-8 backdrop-blur-sm sm:items-center"
      role="dialog"
      aria-modal="true"
      aria-labelledby="admin-settings-title"
    >
      <section className="max-h-[calc(100vh-4rem)] w-full max-w-2xl overflow-y-auto rounded-lg border border-[var(--border)] bg-[var(--surface)] p-5 shadow-xl">
        <div className="flex items-start justify-between gap-4">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.16em] text-[var(--muted-foreground)]">
              Admin
            </p>
            <h2 id="admin-settings-title" className="mt-1 text-lg font-semibold text-[var(--foreground)]">
              Settings
            </h2>
          </div>
          <Button type="button" variant="ghost" size="icon" aria-label="Close admin settings" onClick={onClose}>
            <X className="h-4 w-4" />
          </Button>
        </div>

        <div className="mt-5 rounded-md border border-[var(--border)] bg-[var(--muted)] p-4">
          <button
            type="button"
            className="flex w-full items-center justify-between gap-3 text-left"
            aria-expanded={dangerOpen}
            onClick={() => onDangerOpenChange(!dangerOpen)}
          >
            <span className="flex min-w-0 items-center gap-3">
              <ShieldAlert className="h-5 w-5 shrink-0 text-red-700" />
              <span>
                <span className="block text-sm font-semibold text-[var(--foreground)]">Danger zone</span>
                <span className="block text-xs text-[var(--muted-foreground)]">
                  Reset database records and generated runtime storage.
                </span>
              </span>
            </span>
            <ChevronDown className={cn("h-4 w-4 shrink-0 transition-transform", dangerOpen && "rotate-180")} />
          </button>

          {dangerOpen ? (
            <div className="mt-4 space-y-4 border-t border-[var(--border)] pt-4">
              <Alert tone="error">
                Reset wipes pipeline runs, stage records, events, extracted results, and generated files under
                storage. Source data under data is not deleted by the backend reset service.
              </Alert>
              <Field label={`Type ${RESET_CONFIRMATION} to confirm`}>
                <Input
                  value={resetConfirm}
                  onChange={(event) => onResetConfirmChange(event.target.value)}
                  placeholder={RESET_CONFIRMATION}
                  autoComplete="off"
                />
              </Field>
              <label className="flex items-start gap-3 rounded-md border border-[var(--border)] bg-[var(--surface)] p-3 text-sm">
                <input
                  type="checkbox"
                  className="mt-1 h-4 w-4 accent-red-700"
                  checked={resetAcknowledged}
                  onChange={(event) => onResetAcknowledgedChange(event.target.checked)}
                />
                <span className="text-[var(--muted-foreground)]">
                  I understand this action cannot be undone from the app.
                </span>
              </label>
              <ToggleRow
                label="Force reset active runs"
                checked={resetForce}
                onChange={onResetForceChange}
              />
              {resetErrorText ? <Alert tone="error">{resetErrorText}</Alert> : null}
              {resetResult ? (
                <Alert tone="success">
                  Reset complete. Truncated {resetResult.truncated_tables.length} tables and recreated{" "}
                  {resetResult.recreated_directories.length} storage directories.
                </Alert>
              ) : null}
              <div className="flex justify-end">
                <Button
                  type="button"
                  variant="destructive"
                  disabled={!canReset || resetPending}
                  onClick={onReset}
                >
                  {resetPending ? <Loader2 className="h-4 w-4 animate-spin" /> : <Trash2 className="h-4 w-4" />}
                  Reset everything
                </Button>
              </div>
            </div>
          ) : null}
        </div>
      </section>
    </div>
  );
}

function PanelHeader({ title, description, icon }: { title: string; description: string; icon: ReactNode }) {
  return (
    <div className="flex items-start justify-between gap-4">
      <div>
        <h2 className="text-sm font-semibold text-[var(--foreground)]">{title}</h2>
        <p className="mt-1 text-xs text-[var(--muted-foreground)]">{description}</p>
      </div>
      <div className="rounded-md border border-[var(--border)] bg-[var(--muted)] p-2 text-[var(--muted-foreground)]">
        {icon}
      </div>
    </div>
  );
}

function Field({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="space-y-1.5">
      <Label>{label}</Label>
      {children}
    </div>
  );
}

function ToggleRow({
  label,
  checked,
  onChange,
}: {
  label: string;
  checked: boolean;
  onChange: (checked: boolean) => void;
}) {
  return (
    <div className="flex items-center justify-between gap-3 text-sm">
      <span className="min-w-0 font-medium text-[var(--foreground)]">{label}</span>
      <Switch aria-label={label} checked={checked} onChange={(event) => onChange(event.target.checked)} />
    </div>
  );
}

function StatusIcon({ status }: { status: string }) {
  const className = "h-5 w-5 shrink-0";
  if (status === "successful") {
    return <CheckCircle2 className={cn(className, "text-emerald-600")} />;
  }
  if (status === "failed") {
    return <XCircle className={cn(className, "text-red-600")} />;
  }
  if (status === "skipped") {
    return <AlertTriangle className={cn(className, "text-amber-600")} />;
  }
  if (status === "running" || status === "queued" || status === "started") {
    return <Loader2 className={cn(className, "animate-spin text-[var(--accent)]")} />;
  }
  return <Circle className={cn(className, "text-[var(--muted-foreground)]")} />;
}

function Metric({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-md border border-[var(--border)] bg-[var(--muted)] px-3 py-2">
      <p className="text-lg font-semibold text-[var(--foreground)]">{value}</p>
      <p className="text-xs text-[var(--muted-foreground)]">{label}</p>
    </div>
  );
}

function MetricBadge({ metrics }: { metrics: Record<string, unknown> }) {
  const count = metrics.count ?? metrics.successful ?? metrics.candidates ?? metrics.mentions ?? metrics.decisions;
  if (count === undefined) {
    return null;
  }
  return <Badge variant="muted">{String(count)}</Badge>;
}

function EmptyState({ text }: { text: string }) {
  return (
    <div className="flex min-h-24 items-center justify-center rounded-md border border-dashed border-[var(--border-strong)] bg-[var(--muted)] px-4 py-6 text-center text-sm text-[var(--muted-foreground)]">
      <Clock3 className="mr-2 h-4 w-4" />
      {text}
    </div>
  );
}

function Alert({ tone, children }: { tone: "error" | "success"; children: ReactNode }) {
  return (
    <p
      className={cn(
        "mt-3 rounded-md border px-3 py-2 text-sm",
        tone === "error"
          ? "border-red-200 bg-red-50 text-red-800"
          : "border-emerald-200 bg-emerald-50 text-emerald-800",
      )}
    >
      {children}
    </p>
  );
}

function formatDate(value: string | null | undefined) {
  if (!value) {
    return "not recorded";
  }
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(new Date(value));
}

function formatCell(value: unknown) {
  if (value === null || value === undefined || value === "") {
    return "-";
  }
  if (typeof value === "object") {
    return JSON.stringify(value);
  }
  return String(value);
}

export default App;
