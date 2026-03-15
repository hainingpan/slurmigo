"""Tests for slurmigo.decide — decision engine."""

import pytest

from slurmigo.decide import Decide
from slurmigo.store import Store


class MockConfig:
    max_resubmits = 3
    max_jobs_per_partition = {"main": 500, "default": 150}
    timeout_multiplier = 1.5
    oom_multiplier = 2.0


@pytest.fixture
def store(tmp_path):
    db = Store(str(tmp_path / "test.db"))
    db.initialize(["a=1", "a=2", "a=3", "a=4", "a=5"])
    return db


@pytest.fixture
def decide(store):
    return Decide(MockConfig(), store)


def _submit_job(store, task_id, job_id, status="PENDING"):
    """Helper: mark a task as submitted with a Slurm job_id."""
    store.update_status(task_id, status, job_id=str(job_id), submit_count=1)


# ──────────────────────────────────────────────────────────────
# update_job_statuses
# ──────────────────────────────────────────────────────────────


class TestUpdateJobStatuses:
    def test_running_job_stays_running(self, decide, store):
        """RUNNING job still in squeue → stays RUNNING."""
        _submit_job(store, 1, 1001, status="RUNNING")
        slurm = {"1001": {"state": "RUNNING", "elapsed": "00:10:00"}}
        decide.update_job_statuses(slurm, {})
        row = store.get_by_status("RUNNING")[0]
        assert row["task_id"] == 1
        assert row["elapsed_time"] == "00:10:00"

    def test_pending_becomes_running(self, decide, store):
        """PENDING job becomes RUNNING when squeue says RUNNING."""
        _submit_job(store, 1, 1001, status="PENDING")
        slurm = {"1001": {"state": "RUNNING", "elapsed": "00:01:00"}}
        decide.update_job_statuses(slurm, {})
        assert store.get_by_status("RUNNING")[0]["task_id"] == 1

    def test_completed_via_sacct(self, decide, store):
        """Job COMPLETED in sacct → COMPLETED, wall_time set."""
        _submit_job(store, 1, 1001, status="RUNNING")
        final = {"1001": ("COMPLETED", "", "00:45:00")}
        decide.update_job_statuses({}, final)
        row = store.get_by_status("COMPLETED")[0]
        assert row["wall_time"] == "00:45:00"

    def test_timeout_becomes_failed(self, decide, store):
        """TIMEOUT in sacct → FAILED with reason TIMEOUT."""
        _submit_job(store, 1, 1001, status="RUNNING")
        final = {"1001": ("TIMEOUT", "TIMEOUT", "01:00:00")}
        decide.update_job_statuses({}, final)
        row = store.get_by_status("FAILED")[0]
        assert row["failure_reason"] == "TIMEOUT"

    def test_oom_becomes_failed(self, decide, store):
        """OUT_OF_MEMORY in sacct → FAILED with reason OUT_OF_MEMORY."""
        _submit_job(store, 1, 1001, status="RUNNING")
        final = {"1001": ("OUT_OF_MEMORY", "OUT_OF_MEMORY", "00:20:00")}
        decide.update_job_statuses({}, final)
        row = store.get_by_status("FAILED")[0]
        assert row["failure_reason"] == "OUT_OF_MEMORY"

    def test_preempted_becomes_failed(self, decide, store):
        """PREEMPTED in sacct → FAILED (decide later sorts resubmission)."""
        _submit_job(store, 1, 1001, status="RUNNING")
        final = {"1001": ("PREEMPTED", "PREEMPTED", "00:05:00")}
        decide.update_job_statuses({}, final)
        row = store.get_by_status("FAILED")[0]
        assert row["failure_reason"] == "PREEMPTED"

    def test_sacct_unknown_is_noop(self, decide, store):
        """sacct UNKNOWN → job status unchanged (no-op)."""
        _submit_job(store, 1, 1001, status="RUNNING")
        final = {"1001": ("UNKNOWN", "", None)}
        decide.update_job_statuses({}, final)
        # Still RUNNING — UNKNOWN does not change anything
        assert len(store.get_by_status("RUNNING")) == 1


# ──────────────────────────────────────────────────────────────
# handle_failures
# ──────────────────────────────────────────────────────────────


class TestHandleFailures:
    def test_preempted_resubmission_no_scale(self, decide, store):
        """PREEMPTED → PENDING_RESUBMISSION without scale flags."""
        store.update_status(1, "FAILED", failure_reason="PREEMPTED", submit_count=1)
        decide.handle_failures()
        row = store.get_by_status("PENDING_RESUBMISSION")[0]
        assert row["task_id"] == 1
        reason = row.get("failure_reason") or ""
        assert "scale" not in reason.lower()

    def test_timeout_resubmission_with_scale_time(self, decide, store):
        """TIMEOUT → PENDING_RESUBMISSION with 'TIMEOUT|scale_time'."""
        store.update_status(1, "FAILED", failure_reason="TIMEOUT", submit_count=1)
        decide.handle_failures()
        row = store.get_by_status("PENDING_RESUBMISSION")[0]
        assert row["failure_reason"] == "TIMEOUT|scale_time"

    def test_oom_resubmission_with_scale_mem(self, decide, store):
        """OOM → PENDING_RESUBMISSION with 'OUT_OF_MEMORY|scale_mem'."""
        store.update_status(1, "FAILED", failure_reason="OUT_OF_MEMORY", submit_count=1)
        decide.handle_failures()
        row = store.get_by_status("PENDING_RESUBMISSION")[0]
        assert row["failure_reason"] == "OUT_OF_MEMORY|scale_mem"

    def test_max_resubmits_exceeded_permanently_failed(self, decide, store):
        """submit_count >= max_resubmits → FAILED_PERMANENTLY."""
        store.update_status(1, "FAILED", failure_reason="TIMEOUT", submit_count=3)
        decide.handle_failures()
        assert len(store.get_by_status("FAILED_PERMANENTLY")) == 1

    def test_nonzero_exit_permanently_failed(self, decide, store):
        """NonZeroExitCode → FAILED_PERMANENTLY (not retriable)."""
        store.update_status(
            1, "FAILED", failure_reason="NonZeroExitCode", submit_count=1
        )
        decide.handle_failures()
        assert len(store.get_by_status("FAILED_PERMANENTLY")) == 1


# ──────────────────────────────────────────────────────────────
# get_jobs_to_submit
# ──────────────────────────────────────────────────────────────


class TestGetJobsToSubmit:
    def test_priority_ordering_scaled_first(self, decide, store):
        """Priority: scaled resubmissions > non-scaled > new jobs."""
        # task 1: scaled (TIMEOUT|scale_time)
        store.update_status(
            1, "PENDING_RESUBMISSION", failure_reason="TIMEOUT|scale_time"
        )
        # task 2: non-scaled (preemption)
        store.update_status(2, "PENDING_RESUBMISSION", failure_reason="PREEMPTED")
        # task 3-5: NOT_SUBMITTED (the default)

        jobs = decide.get_jobs_to_submit(queue_counts={}, partition="main")
        ids = [j["task_id"] for j in jobs]
        # scaled first, then non-scaled, then not-submitted
        assert ids[0] == 1  # scaled
        assert ids[1] == 2  # non-scaled resubmission
        assert ids[2:] == [3, 4, 5]  # new

    def test_respects_partition_slot_limits(self, decide, store):
        """Slots = max_for_partition - currently_queued."""
        # 498 jobs already in queue for "main" (limit 500), so 2 slots
        jobs = decide.get_jobs_to_submit(queue_counts={"main": 498}, partition="main")
        assert len(jobs) == 2

    def test_no_slots_returns_empty(self, decide, store):
        jobs = decide.get_jobs_to_submit(queue_counts={"main": 500}, partition="main")
        assert jobs == []

    def test_default_partition_limit(self, decide, store):
        """Unknown partition falls back to 'default' limit (150)."""
        jobs = decide.get_jobs_to_submit(queue_counts={"gpu": 149}, partition="gpu")
        assert len(jobs) == 1


# ──────────────────────────────────────────────────────────────
# calculate_scaled_resources
# ──────────────────────────────────────────────────────────────


class TestCalculateScaledResources:
    def test_scale_time(self, decide, store):
        """01:00:00 × 1.5 = 01:30:00."""
        store.update_status(
            1,
            "PENDING_RESUBMISSION",
            failure_reason="TIMEOUT|scale_time",
            original_time_limit="01:00:00",
        )
        result = decide.calculate_scaled_resources(1)
        assert result == {"time": "01:30:00"}

    def test_scale_memory(self, decide, store):
        """4G × 2.0 = 8G."""
        store.update_status(
            1,
            "PENDING_RESUBMISSION",
            failure_reason="OUT_OF_MEMORY|scale_mem",
            original_mem="4G",
        )
        result = decide.calculate_scaled_resources(1)
        assert result == {"mem": "8G"}

    def test_no_scale_flag_returns_empty(self, decide, store):
        """Job without scale flags → empty dict."""
        store.update_status(1, "PENDING_RESUBMISSION", failure_reason="PREEMPTED")
        result = decide.calculate_scaled_resources(1)
        assert result == {}
