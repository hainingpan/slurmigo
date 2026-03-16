"""Integration tests against real Slurm. No mocks.

Run: pytest tests/integration/ -v -s
Skip: pytest -m "not integration"
Takes ~5 minutes (real jobs on debug queue).
"""

from __future__ import annotations

import os
import subprocess
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "src"))

from slurmigo import act, sense
from slurmigo.decide import Decide
from slurmigo.store import Store

pytestmark = pytest.mark.integration

PYTHON = "/home/hp636/.pyenv/versions/miniforge3-25.3.1-0/bin/python"
PARTITION = "main"
POLL_INTERVAL = 5
MAX_WAIT = 300


class TestConfig:
    def __init__(self, **kwargs):
        self.max_resubmits = kwargs.get("max_resubmits", 3)
        self.max_jobs_per_partition = kwargs.get(
            "max_jobs_per_partition", {"default": 500}
        )
        self.timeout_multiplier = kwargs.get("timeout_multiplier", 1.5)
        self.oom_multiplier = kwargs.get("oom_multiplier", 1.5)


def write_params(tmpdir, lines):
    path = os.path.join(str(tmpdir), "params.txt")
    with open(path, "w") as f:
        for line in lines:
            f.write(line + "\n")
    return path


def write_script(tmpdir, body, directives):
    path = os.path.join(str(tmpdir), "run.sh")
    with open(path, "w") as f:
        f.write("#!/bin/bash\n")
        for directive in directives:
            f.write(f"#SBATCH {directive}\n")
        f.write("\n")
        f.write(body + "\n")
    os.chmod(path, 0o755)
    return path


def cancel_all_jobs(store):
    job_ids = sorted(
        {str(row["job_id"]) for row in store.get_all() if row.get("job_id")}
    )
    if not job_ids:
        return
    try:
        subprocess.run(["scancel"] + job_ids, capture_output=True, timeout=30)
    except Exception:
        pass


def run_pipeline_until_done(
    store,
    config,
    script_path,
    params,
    state_dir,
    max_wait=MAX_WAIT,
):
    directives = sense.parse_sbatch_directives(script_path)
    partition = directives.get("partition", PARTITION)

    original_time = directives.get("time")
    original_mem = directives.get("mem")
    if original_time or original_mem:
        for row in store.get_by_status("NOT_SUBMITTED"):
            task_id = int(row["task_id"])
            store.update_status(
                task_id,
                "NOT_SUBMITTED",
                original_time_limit=original_time,
                original_mem=original_mem,
                current_time_limit=original_time,
                current_mem=original_mem,
            )

    decide_engine = Decide(config, store)
    deadline = time.time() + max_wait

    while time.time() < deadline:
        slurm_details = sense.get_job_details()

        tracked_ids = {
            str(row["job_id"])
            for row in (store.get_by_status("PENDING") + store.get_by_status("RUNNING"))
            if row.get("job_id")
        }
        missing_ids = tracked_ids - set(slurm_details.keys())
        final_statuses = {
            job_id: sense.get_final_status(job_id) for job_id in missing_ids
        }

        decide_engine.update_job_statuses(slurm_details, final_statuses)
        decide_engine.handle_failures()

        if store.is_all_finished():
            return

        queue_counts = sense.get_queue_per_partition()
        jobs_to_submit = decide_engine.get_jobs_to_submit(queue_counts, partition)

        for row in jobs_to_submit:
            task_id = int(row["task_id"])
            param_dict = params[task_id - 1] if task_id <= len(params) else {}

            overrides = decide_engine.calculate_scaled_resources(task_id)
            sbatch_args = act.build_sbatch_args(directives, overrides)
            wrapper = act.generate_wrapper(task_id, param_dict, script_path, state_dir)
            job_id = act.submit_job(wrapper, sbatch_args)

            if not job_id:
                pytest.fail(f"sbatch failed for task_id={task_id}")

            submit_count = int(row.get("submit_count") or 0) + 1
            store.update_status(
                task_id,
                "PENDING",
                job_id=job_id,
                submit_count=submit_count,
            )

        time.sleep(POLL_INTERVAL)

    statuses = {r["task_id"]: r["status"] for r in store.get_all()}
    pytest.fail(f"Timed out after {max_wait}s. Current statuses: {statuses}")


@pytest.fixture(autouse=True)
def cleanup(tmp_path):
    state_dir = str(tmp_path / ".slurmigo")
    os.makedirs(os.path.join(state_dir, "wrappers"), exist_ok=True)
    yield state_dir
    try:
        db = os.path.join(state_dir, "state.db")
        if os.path.exists(db):
            s = Store(db)
            cancel_all_jobs(s)
    except Exception:
        pass


def test_code_error_fails_permanently(tmp_path, cleanup):
    params_path = write_params(tmp_path, ["alpha=0.1"])
    script_path = write_script(
        tmp_path,
        body="exit 1",
        directives=[
            "--partition=main",
            "--time=00:01:00",
            "--mem=100M",
        ],
    )

    params, _ = act.parse_params_file(params_path)
    state_db = os.path.join(cleanup, "state.db")
    store = Store(state_db)
    store.initialize([str(p) for p in params])

    config = TestConfig(max_resubmits=3)
    run_pipeline_until_done(store, config, script_path, params, cleanup)

    row = store.get_all()[0]
    assert row["status"] == "FAILED_PERMANENTLY", (
        f"Got {row['status']}, reason={row.get('failure_reason')}"
    )
    assert row["submit_count"] == 1, f"Got submit_count={row['submit_count']}"
    print(
        f"PASS: code error -> FAILED_PERMANENTLY, submit_count=1, reason={row.get('failure_reason')}"
    )


def test_timeout_triggers_resubmission(tmp_path, cleanup):
    params_path = write_params(tmp_path, ["alpha=0.1"])
    script_path = write_script(
        tmp_path,
        body="sleep 90",
        directives=[
            "--partition=main",
            "--time=00:01:00",
            "--mem=100M",
        ],
    )

    params, _ = act.parse_params_file(params_path)
    state_db = os.path.join(cleanup, "state.db")
    store = Store(state_db)
    store.initialize([str(p) for p in params])

    config = TestConfig(max_resubmits=3, timeout_multiplier=2.0)
    run_pipeline_until_done(store, config, script_path, params, cleanup, max_wait=900)

    row = store.get_all()[0]
    assert row["status"] == "COMPLETED", (
        f"Got {row['status']}, reason={row.get('failure_reason')}"
    )
    assert row["submit_count"] >= 2, f"Got submit_count={row['submit_count']}"
    assert row["current_time_limit"] == "00:02:00", f"Got {row['current_time_limit']}"
    print(
        f"PASS: timeout -> resubmit -> COMPLETED, count={row['submit_count']}, time={row['current_time_limit']}"
    )


def test_oom_triggers_resubmission(tmp_path, cleanup):
    params_path = write_params(tmp_path, ["alpha=0.1"])
    script_path = write_script(
        tmp_path,
        body=f'{PYTHON} -c "x = bytearray(100*1024*1024)"',
        directives=[
            "--partition=main",
            "--time=00:02:00",
            "--mem=10M",
        ],
    )

    params, _ = act.parse_params_file(params_path)
    state_db = os.path.join(cleanup, "state.db")
    store = Store(state_db)
    store.initialize([str(p) for p in params])

    config = TestConfig(max_resubmits=3, oom_multiplier=3.0)
    run_pipeline_until_done(store, config, script_path, params, cleanup)

    row = store.get_all()[0]
    if row["status"] == "COMPLETED" and row["submit_count"] == 1:
        pytest.skip("cgroups not enforced")
    assert row["submit_count"] >= 2, f"Got submit_count={row['submit_count']}"
    print(
        f"PASS: OOM -> resubmit, count={row['submit_count']}, mem={row.get('current_mem')}"
    )


def test_happy_path_completes(tmp_path, cleanup):
    params_path = write_params(
        tmp_path,
        ["alpha=0.1", "alpha=0.2", "alpha=0.3"],
    )
    script_path = write_script(
        tmp_path,
        body='echo "task $SLURMIGO_TASK_ID alpha=$alpha done"',
        directives=[
            "--partition=main",
            "--time=00:01:00",
            "--mem=100M",
        ],
    )

    params, _ = act.parse_params_file(params_path)
    state_db = os.path.join(cleanup, "state.db")
    store = Store(state_db)
    store.initialize([str(p) for p in params])

    config = TestConfig(max_resubmits=3)
    run_pipeline_until_done(store, config, script_path, params, cleanup)

    rows = {r["task_id"]: r for r in store.get_all()}
    for task_id in [1, 2, 3]:
        assert rows[task_id]["status"] == "COMPLETED", (
            f"Task {task_id}: {rows[task_id]['status']}"
        )
        assert rows[task_id]["wall_time"] is not None
    print("PASS: all 3 tasks COMPLETED")
