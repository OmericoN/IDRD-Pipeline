from __future__ import annotations

from collections import deque
from contextlib import nullcontext
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from threading import RLock
from typing import Dict, List, Optional
import json
import sys


PIPELINE_STAGES = ("fetch", "download", "convert", "render")
RESULT_STATUSES = ("successful", "failed", "skipped")


@dataclass
class StageCounters:
    queued: int = 0
    running: int = 0
    successful: int = 0
    failed: int = 0
    skipped: int = 0
    active_items: set[str] = field(default_factory=set)


class PipelineRuntimeState:
    """Thread-safe runtime state for pipeline stage visibility and event logging."""

    def __init__(self, event_log_path: Optional[Path] = None, error_buffer_size: int = 30):
        self._lock = RLock()
        self._started_at = datetime.now()
        self._stages: Dict[str, StageCounters] = {name: StageCounters() for name in PIPELINE_STAGES}
        self._recent_errors: deque[dict] = deque(maxlen=error_buffer_size)
        self._event_log_path = event_log_path
        if self._event_log_path:
            self._event_log_path.parent.mkdir(parents=True, exist_ok=True)

    @property
    def event_log_path(self) -> Optional[Path]:
        return self._event_log_path

    def queue_items(self, stage: str, count: int):
        with self._lock:
            counters = self._stages[stage]
            counters.queued += max(count, 0)
        self._write_event({"event": "queued", "stage": stage, "count": count})

    def task_started(self, stage: str, item_id: Optional[str] = None):
        with self._lock:
            counters = self._stages[stage]
            if counters.queued > 0:
                counters.queued -= 1
            counters.running += 1
            if item_id:
                counters.active_items.add(item_id)
        self._write_event({"event": "started", "stage": stage, "item_id": item_id})

    def task_finished(
        self,
        stage: str,
        status: str,
        item_id: Optional[str] = None,
        message: Optional[str] = None,
    ):
        if status not in RESULT_STATUSES:
            raise ValueError(f"Unsupported status: {status}")

        with self._lock:
            counters = self._stages[stage]
            if counters.running > 0:
                counters.running -= 1
            if item_id:
                counters.active_items.discard(item_id)
            current = getattr(counters, status)
            setattr(counters, status, current + 1)

            if status == "failed":
                self._recent_errors.append(
                    {
                        "timestamp": datetime.now().isoformat(timespec="seconds"),
                        "stage": stage,
                        "item_id": item_id,
                        "message": message or "Unknown error",
                    }
                )

        self._write_event(
            {
                "event": "finished",
                "stage": stage,
                "item_id": item_id,
                "status": status,
                "message": message,
            }
        )

    def snapshot(self) -> dict:
        with self._lock:
            stages = {}
            for stage, counters in self._stages.items():
                stages[stage] = {
                    "queued": counters.queued,
                    "running": counters.running,
                    "successful": counters.successful,
                    "failed": counters.failed,
                    "skipped": counters.skipped,
                    "active_items": sorted(counters.active_items),
                }

            return {
                "started_at": self._started_at.isoformat(timespec="seconds"),
                "elapsed_seconds": (datetime.now() - self._started_at).total_seconds(),
                "stages": stages,
                "recent_errors": list(self._recent_errors),
                "event_log_path": str(self._event_log_path) if self._event_log_path else None,
            }

    def _write_event(self, payload: dict):
        if not self._event_log_path:
            return

        event = {
            "timestamp": datetime.now().isoformat(timespec="milliseconds"),
            **payload,
        }
        with self._lock:
            with self._event_log_path.open("a", encoding="utf-8") as f:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")


class RuntimeDashboardView:
    """Rich renderable backed by PipelineRuntimeState snapshots."""

    def __init__(self, state: PipelineRuntimeState):
        self._state = state

    def __rich__(self):
        from rich.console import Group
        from rich.panel import Panel
        from rich.table import Table
        from rich.text import Text

        snapshot = self._state.snapshot()
        stage_table = Table(title="Pipeline Runtime State", show_lines=False, expand=True)
        stage_table.add_column("Stage", style="cyan", no_wrap=True)
        stage_table.add_column("Queued", justify="right")
        stage_table.add_column("Running", justify="right")
        stage_table.add_column("Successful", justify="right")
        stage_table.add_column("Skipped", justify="right")
        stage_table.add_column("Failed", justify="right")
        stage_table.add_column("Active Items", overflow="fold")

        for stage in PIPELINE_STAGES:
            counters = snapshot["stages"][stage]
            active = ", ".join(counters["active_items"][:4])
            if len(counters["active_items"]) > 4:
                active += ", ..."
            stage_table.add_row(
                stage.upper(),
                str(counters["queued"]),
                str(counters["running"]),
                str(counters["successful"]),
                str(counters["skipped"]),
                str(counters["failed"]),
                active or "-",
            )

        errors_table = Table(title="Recent Failures", expand=True)
        errors_table.add_column("Time", style="dim", no_wrap=True)
        errors_table.add_column("Stage", style="yellow", no_wrap=True)
        errors_table.add_column("Item", no_wrap=True)
        errors_table.add_column("Message", overflow="fold")
        for error in snapshot["recent_errors"][-8:]:
            errors_table.add_row(
                error["timestamp"],
                error["stage"],
                error.get("item_id") or "-",
                error.get("message") or "-",
            )
        if not snapshot["recent_errors"]:
            errors_table.add_row("-", "-", "-", "No failures recorded.")

        footer = Text(
            f"Elapsed: {snapshot['elapsed_seconds']:.1f}s | Events: {snapshot['event_log_path'] or 'disabled'}",
            style="bold green",
        )
        return Panel(Group(stage_table, errors_table, footer), title="Concurrent Pipeline Monitor", border_style="green")


def rich_is_available() -> bool:
    try:
        import rich  # noqa: F401

        return True
    except Exception:
        return False


def should_enable_live_monitor(mode: str, monitor_mode: str) -> bool:
    if monitor_mode == "off":
        return False
    if monitor_mode == "live":
        return True
    return mode == "concurrent"


class RuntimeLiveSession:
    """Optional live dashboard session around PipelineRuntimeState."""

    def __init__(self, state: PipelineRuntimeState, enabled: bool, refresh_seconds: float = 0.3):
        self._state = state
        self._enabled = enabled
        self._refresh_seconds = max(refresh_seconds, 0.1)
        self._ctx = nullcontext()

    def __enter__(self):
        if not self._enabled:
            return self

        if not rich_is_available():
            print("  [WARN] Rich monitor requested but 'rich' is not installed. Continuing without live dashboard.")
            return self

        if not sys.stdout.isatty():
            print("  [WARN] Live dashboard disabled (non-interactive terminal).")
            return self

        from rich.live import Live

        view = RuntimeDashboardView(self._state)
        refresh_rate = max(1, int(round(1.0 / self._refresh_seconds)))
        self._ctx = Live(view, refresh_per_second=refresh_rate, transient=False)
        self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._ctx.__exit__(exc_type, exc_val, exc_tb)
