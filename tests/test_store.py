"""Tests for slurmigo.store — SQLite CRUD layer."""

import json
import os

import pytest

from slurmigo.store import Store


@pytest.fixture
def store(tmp_path):
    """Create a fresh Store with an isolated temp DB."""
    db_path = str(tmp_path / "test.db")
    return Store(db_path)


@pytest.fixture
def populated_store(store):
    """Store pre-loaded with 5 param lines."""
    store.initialize(["alpha=0.1", "alpha=0.2", "alpha=0.3", "alpha=0.4", "alpha=0.5"])
    return store


class TestInitialize:
    def test_inserts_correct_number_of_rows(self, store):
        lines = ["a=1", "b=2", "c=3"]
        store.initialize(lines)
        assert len(store.get_all()) == 3

    def test_task_ids_are_one_indexed(self, store):
        store.initialize(["x=1", "x=2"])
        rows = store.get_all()
        ids = [r["task_id"] for r in rows]
        assert ids == [1, 2]

    def test_idempotent_noop_if_rows_exist(self, populated_store):
        populated_store.initialize(["extra=1", "extra=2"])
        assert len(populated_store.get_all()) == 5  # unchanged

    def test_all_default_to_not_submitted(self, populated_store):
        for row in populated_store.get_all():
            assert row["status"] == "NOT_SUBMITTED"


class TestGetCounts:
    def test_returns_accurate_status_distribution(self, populated_store):
        counts = populated_store.get_counts()
        assert counts == {"NOT_SUBMITTED": 5}

    def test_reflects_updates(self, populated_store):
        populated_store.update_status(1, "RUNNING")
        populated_store.update_status(2, "COMPLETED")
        counts = populated_store.get_counts()
        assert counts["RUNNING"] == 1
        assert counts["COMPLETED"] == 1
        assert counts["NOT_SUBMITTED"] == 3


class TestGetByStatus:
    def test_filters_correctly(self, populated_store):
        populated_store.update_status(1, "RUNNING")
        populated_store.update_status(2, "RUNNING")
        populated_store.update_status(3, "COMPLETED")
        running = populated_store.get_by_status("RUNNING")
        assert len(running) == 2
        assert all(r["status"] == "RUNNING" for r in running)

    def test_returns_empty_for_absent_status(self, populated_store):
        assert populated_store.get_by_status("FAILED") == []


class TestUpdateStatus:
    def test_updates_status(self, populated_store):
        populated_store.update_status(1, "RUNNING")
        row = populated_store.get_by_status("RUNNING")[0]
        assert row["task_id"] == 1

    def test_updates_multiple_fields_via_kwargs(self, populated_store):
        populated_store.update_status(
            1, "FAILED", failure_reason="TIMEOUT", wall_time="01:00:00"
        )
        row = [r for r in populated_store.get_all() if r["task_id"] == 1][0]
        assert row["status"] == "FAILED"
        assert row["failure_reason"] == "TIMEOUT"
        assert row["wall_time"] == "01:00:00"


class TestShorthandReads:
    def test_get_running(self, populated_store):
        populated_store.update_status(1, "RUNNING")
        populated_store.update_status(2, "RUNNING")
        assert len(populated_store.get_running()) == 2

    def test_get_failed_includes_both_types(self, populated_store):
        populated_store.update_status(1, "FAILED")
        populated_store.update_status(2, "FAILED_PERMANENTLY")
        populated_store.update_status(3, "COMPLETED")
        failed = populated_store.get_failed()
        assert len(failed) == 2
        statuses = {r["status"] for r in failed}
        assert statuses == {"FAILED", "FAILED_PERMANENTLY"}

    def test_get_pending(self, populated_store):
        populated_store.update_status(1, "PENDING")
        assert len(populated_store.get_pending()) == 1

    def test_get_completed(self, populated_store):
        populated_store.update_status(1, "COMPLETED")
        assert len(populated_store.get_completed()) == 1


class TestIsAllFinished:
    def test_false_when_jobs_still_active(self, populated_store):
        populated_store.update_status(1, "COMPLETED")
        assert populated_store.is_all_finished() is False

    def test_true_when_all_terminal(self, populated_store):
        for i in range(1, 6):
            if i <= 3:
                populated_store.update_status(i, "COMPLETED")
            else:
                populated_store.update_status(i, "FAILED_PERMANENTLY")
        assert populated_store.is_all_finished() is True

    def test_false_with_running(self, populated_store):
        for i in range(1, 5):
            populated_store.update_status(i, "COMPLETED")
        populated_store.update_status(5, "RUNNING")
        assert populated_store.is_all_finished() is False


class TestImportFromJson:
    def test_migrates_old_state_format(self, tmp_path):
        json_path = str(tmp_path / "old_state.json")
        old_state = {
            "1": {
                "params": "alpha=0.1",
                "status": "COMPLETED",
                "job_id": "12345",
                "submit_count": 1,
                "failure_reason": None,
                "elapsed_time": None,
                "wall_time": "00:30:00",
            },
            "2": {
                "params": "alpha=0.2",
                "status": "FAILED",
                "job_id": "12346",
                "submit_count": 2,
                "failure_reason": "TIMEOUT",
                "elapsed_time": None,
                "wall_time": "01:00:00",
            },
        }
        with open(json_path, "w") as f:
            json.dump(old_state, f)

        db_path = str(tmp_path / "migrated.db")
        store = Store(db_path)
        store.import_from_json(json_path)

        rows = store.get_all()
        assert len(rows) == 2

        r1 = [r for r in rows if r["task_id"] == 1][0]
        assert r1["status"] == "COMPLETED"
        assert r1["job_id"] == "12345"
        assert r1["wall_time"] == "00:30:00"

        r2 = [r for r in rows if r["task_id"] == 2][0]
        assert r2["status"] == "FAILED"
        assert r2["failure_reason"] == "TIMEOUT"

    def test_import_skips_existing_rows(self, tmp_path):
        json_path = str(tmp_path / "state.json")
        state = {
            "1": {"params": "a=1", "status": "COMPLETED", "submit_count": 1},
        }
        with open(json_path, "w") as f:
            json.dump(state, f)

        db_path = str(tmp_path / "dup.db")
        store = Store(db_path)
        store.import_from_json(json_path)
        store.import_from_json(json_path)  # second call — should be no-op
        assert len(store.get_all()) == 1
