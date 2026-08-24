import { createContext, useCallback, useContext, useMemo, useRef, useState, type ReactNode } from "react";
import { useQueryClient } from "@tanstack/react-query";
import { useLocation, useNavigate } from "react-router";

import {
  api,
  type DiscoveryPreview,
  type DiscoveryPreviewRequest,
  type RunCreateRequest,
} from "@/shared/api/client";

type LaunchPhase = "idle" | "discovering" | "starting" | "failed";

export type LaunchJobRequest = {
  preview: DiscoveryPreview | undefined;
  previewRequest: DiscoveryPreviewRequest;
  runRequest: Omit<RunCreateRequest, "preview_id" | "excluded_candidate_ids">;
  excludedCandidateIds: string[];
};

type LaunchJobContextValue = {
  error: Error | null;
  isPending: boolean;
  phase: LaunchPhase;
  start: (request: LaunchJobRequest) => void;
};

const LaunchJobContext = createContext<LaunchJobContextValue | null>(null);

export function LaunchJobProvider({ children }: { children: ReactNode }) {
  const queryClient = useQueryClient();
  const navigate = useNavigate();
  const location = useLocation();
  const locationRef = useRef(location.pathname);
  const inFlightRef = useRef(false);
  const [phase, setPhase] = useState<LaunchPhase>("idle");
  const [error, setError] = useState<Error | null>(null);

  locationRef.current = location.pathname;

  const start = useCallback((request: LaunchJobRequest) => {
    if (inFlightRef.current) return;

    inFlightRef.current = true;
    setError(null);
    setPhase(request.preview ? "starting" : "discovering");

    void (async () => {
      try {
        const preview = request.preview ?? await api.discoveryPreview(request.previewRequest);
        setPhase("starting");
        const created = await api.createRun({
          ...request.runRequest,
          preview_id: preview.preview_id,
          excluded_candidate_ids: request.excludedCandidateIds,
        });

        setPhase("idle");
        void queryClient.invalidateQueries({ queryKey: ["runs"] });
        if (locationRef.current === "/launch") {
          navigate(`/runs/${created.pipeline_run_id}`);
        }
      } catch (cause) {
        setError(cause instanceof Error ? cause : new Error("The discovery launch failed."));
        setPhase("failed");
      } finally {
        inFlightRef.current = false;
      }
    })();
  }, [navigate, queryClient]);

  const value = useMemo<LaunchJobContextValue>(() => ({
    error,
    isPending: phase === "discovering" || phase === "starting",
    phase,
    start,
  }), [error, phase, start]);

  return <LaunchJobContext.Provider value={value}>{children}</LaunchJobContext.Provider>;
}

export function useLaunchJob() {
  const context = useContext(LaunchJobContext);
  if (!context) throw new Error("useLaunchJob must be used inside LaunchJobProvider.");
  return context;
}
