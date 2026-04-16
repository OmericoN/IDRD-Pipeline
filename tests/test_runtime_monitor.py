from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from utils.runtime_monitor import (
    PipelineRuntimeState,
    should_enable_live_monitor,
)


def test_runtime_state_counts_and_active_items(tmp_path):
    state = PipelineRuntimeState(event_log_path=tmp_path / "events.jsonl")
    state.queue_items("download", 2)
    state.task_started("download", "p1")
    state.task_started("download", "p2")
    state.task_finished("download", "successful", "p1", "ok")
    state.task_finished("download", "failed", "p2", "boom")

    snapshot = state.snapshot()
    download = snapshot["stages"]["download"]
    assert download["queued"] == 0
    assert download["running"] == 0
    assert download["successful"] == 1
    assert download["failed"] == 1
    assert download["active_items"] == []
    assert snapshot["recent_errors"][-1]["item_id"] == "p2"
    assert Path(snapshot["event_log_path"]).exists()


def test_runtime_state_skipped_and_queued_floor():
    state = PipelineRuntimeState()
    state.task_started("render", "p1")
    state.task_finished("render", "skipped", "p1", "Already exists: p1.md")

    snapshot = state.snapshot()
    render = snapshot["stages"]["render"]
    assert render["queued"] == 0
    assert render["running"] == 0
    assert render["skipped"] == 1


def test_should_enable_live_monitor_modes():
    assert should_enable_live_monitor("concurrent", "auto") is True
    assert should_enable_live_monitor("sequential", "auto") is False
    assert should_enable_live_monitor("sequential", "live") is True
    assert should_enable_live_monitor("concurrent", "off") is False
