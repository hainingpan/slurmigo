"""Integration tests for slurmigo against real Slurm on the debug queue.

Run with:
    pytest -m integration tests/integration/
Skip with:
    pytest -m "not integration"
"""

import os
import sqlite3
import subprocess
import textwrap

import pytest

pytestmark = pytest.mark.integration

PYTHON = "/home/hp636/.pyenv/versions/miniforge3-25.3.1-0/bin/python"
SLURMIGO = [PYTHON, "-m", "slurmigo"]
TEST_TIMEOUT = 300


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def write_params(tmpdir, lines):
    """Write params.txt with given lines."""
    path = os.path.join(str(tmpdir), "params.txt")
    with open(path, "w") as f:
        for line in lines:
            f.write(line + "\n")
    return path


def write_script(tmpdir, content, directives):
    """Write an sbatch script with given #SBATCH directives and body.

    Returns the path to the script file.
    """
    path = os.path.join(str(tmpdir), "run.sh")
    with open(path, "w") as f:
        f.write("#!/bin/bash\n")
        for directive in directives:
            f.write(f"#SBATCH {directive}\n")
        f.write("\n")
        f.write(content + "\n")
    os.chmod(path, 0o755)
    return path


def write_config(tmpdir, params_path, script_path, **kwargs):
    """Write a slurmigo.toml config file. Returns path to config."""
    path = os.path.join(str(tmpdir), "slurmigo.toml")
    lines = []
    lines.append(f'params_file = "{params_path}"')
    lines.append(f'script = "{script_path}"')
    state_dir = os.path.join(str(tmpdir), ".slurmigo")
    lines.append(f'state_dir = "{state_dir}"')
    lines.append("check_interval = 5")

    for key, value in kwargs.items():
        if isinstance(value, str):
            lines.append(f'{key} = "{value}"')
        elif isinstance(value, dict):
            inner = ", ".join(
                f'"{k}" = {v}' if isinstance(k, str) else f"{k} = {v}"
                for k, v in value.items()
            )
            lines.append(f"{key} = {{{inner}}}")
        else:
            lines.append(f"{key} = {value}")

    with open(path, "w") as f:
        f.write("\n".join(lines) + "\n")
    return path, state_dir


def run_slurmigo(
    tmpdir,
    params_path,
    script_path,
    timeout=TEST_TIMEOUT,
    extra_args=None,
    **config_kwargs,
):
    """Run slurmigo and wait for completion.

    Returns (returncode, state_db_path).
    Creates a temp dir, writes slurmigo.toml, runs slurmigo.
    Returns when all jobs finish or timeout reached.
    """
    config_path, state_dir = write_config(
        tmpdir, params_path, script_path, **config_kwargs
    )
    cmd = SLURMIGO + ["--config", config_path]
    if extra_args:
        cmd.extend(extra_args)

    env = os.environ.copy()
    env["TERM"] = "dumb"

    proc = subprocess.run(
        cmd,
        timeout=timeout,
        capture_output=True,
        text=True,
        env=env,
    )

    state_db = os.path.join(state_dir, "state.db")
    return proc.returncode, state_db, proc.stdout, proc.stderr


def get_final_statuses(state_db_path):
    """Read the SQLite state.db and return {task_id: status}."""
    conn = sqlite3.connect(state_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT task_id, status FROM jobs ORDER BY task_id").fetchall()
    result = {row["task_id"]: row["status"] for row in rows}
    conn.close()
    return result


def get_job_details(state_db_path):
    """Read the SQLite state.db and return {task_id: dict} with full row data."""
    conn = sqlite3.connect(state_db_path)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM jobs ORDER BY task_id").fetchall()
    result = {row["task_id"]: dict(row) for row in rows}
    conn.close()
    return result


def _get_our_job_ids(state_db_path):
    """Extract all Slurm job IDs from the state database for cleanup."""
    if not os.path.exists(state_db_path):
        return []
    try:
        conn = sqlite3.connect(state_db_path)
        rows = conn.execute(
            "SELECT job_id FROM jobs WHERE job_id IS NOT NULL"
        ).fetchall()
        conn.close()
        return [str(row[0]) for row in rows if row[0]]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Cleanup fixture
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def cleanup_slurm_jobs(tmp_path):
    """Cancel any leftover jobs after each test."""
    state_db = os.path.join(str(tmp_path), ".slurmigo", "state.db")
    yield
    job_ids = _get_our_job_ids(state_db)
    if job_ids:
        try:
            subprocess.run(
                ["scancel"] + job_ids,
                capture_output=True,
                timeout=30,
            )
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Test 1: Code error → FAILED_PERMANENTLY
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_code_error_fails_permanently(tmp_path):
    """A script that exits 1 should be marked FAILED_PERMANENTLY, not resubmitted."""
    params_path = write_params(tmp_path, ["alpha=0.1"])
    script_path = write_script(
        tmp_path,
        content="exit 1",
        directives=[
            "--partition=debug",
            "--time=00:01:00",
            "--mem=100M",
        ],
    )

    returncode, state_db, stdout, stderr = run_slurmigo(
        tmp_path,
        params_path,
        script_path,
        max_resubmits=3,
        timeout_multiplier=1.5,
    )

    assert os.path.exists(state_db), f"state.db not found at {state_db}"

    statuses = get_final_statuses(state_db)
    assert statuses[1] == "FAILED_PERMANENTLY", (
        f"Expected task 1 to be FAILED_PERMANENTLY, got {statuses[1]}"
    )

    details = get_job_details(state_db)
    assert details[1]["submit_count"] == 1, (
        f"Expected submit_count=1 (no retries for code error), got {details[1]['submit_count']}"
    )


# ---------------------------------------------------------------------------
# Test 2: TIMEOUT → resubmit with scaled wall time
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_timeout_triggers_resubmission(tmp_path):
    """A job that times out should be resubmitted with scaled wall time and then complete."""
    params_path = write_params(tmp_path, ["alpha=0.1"])
    script_path = write_script(
        tmp_path,
        content="sleep 45",
        directives=[
            "--partition=debug",
            "--time=00:00:30",
            "--mem=100M",
        ],
    )

    returncode, state_db, stdout, stderr = run_slurmigo(
        tmp_path,
        params_path,
        script_path,
        max_resubmits=3,
        timeout_multiplier=2.0,
    )

    assert os.path.exists(state_db), f"state.db not found at {state_db}"

    statuses = get_final_statuses(state_db)
    assert statuses[1] == "COMPLETED", (
        f"Expected task 1 to be COMPLETED after resubmission, got {statuses[1]}"
    )

    details = get_job_details(state_db)
    assert details[1]["submit_count"] >= 2, (
        f"Expected at least 2 submissions (original + resubmit), got {details[1]['submit_count']}"
    )
    assert details[1]["current_time_limit"] == "00:01:00", (
        f"Expected scaled time limit 00:01:00 (30s * 2.0), got {details[1]['current_time_limit']}"
    )


# ---------------------------------------------------------------------------
# Test 3: OOM → resubmit with scaled memory (conditional)
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_oom_triggers_resubmission(tmp_path):
    """A job that runs out of memory should be resubmitted with scaled memory."""
    params_path = write_params(tmp_path, ["alpha=0.1"])
    script_path = write_script(
        tmp_path,
        content=f'{PYTHON} -c "x = bytearray(100*1024*1024)"',
        directives=[
            "--partition=debug",
            "--time=00:01:00",
            "--mem=10M",
        ],
    )

    returncode, state_db, stdout, stderr = run_slurmigo(
        tmp_path,
        params_path,
        script_path,
        max_resubmits=3,
        oom_multiplier=3.0,
    )

    assert os.path.exists(state_db), f"state.db not found at {state_db}"

    statuses = get_final_statuses(state_db)
    details = get_job_details(state_db)

    if statuses[1] == "COMPLETED" and details[1]["submit_count"] == 1:
        pytest.skip(
            "OOM not triggered — memory cgroups may not be enforced on this cluster"
        )

    assert details[1]["submit_count"] >= 2, (
        f"Expected at least 2 submissions after OOM, got {details[1]['submit_count']}"
    )

    if details[1]["current_mem"] is not None:
        assert details[1]["current_mem"] != details[1]["original_mem"], (
            "Expected current_mem to differ from original_mem after OOM scaling"
        )

    assert statuses[1] in ("COMPLETED", "FAILED_PERMANENTLY"), (
        f"Expected COMPLETED or FAILED_PERMANENTLY, got {statuses[1]}"
    )


# ---------------------------------------------------------------------------
# Test 4: Happy path → COMPLETED
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_happy_path_completes(tmp_path):
    """A simple job should complete successfully."""
    params_path = write_params(
        tmp_path,
        [
            "alpha=0.1",
            "alpha=0.2",
            "alpha=0.3",
        ],
    )
    script_path = write_script(
        tmp_path,
        content=textwrap.dedent("""\
            PARAMS=$(sed -n "${REAL_TASK_ID}p" "$PARAMS_FILE")
            eval $PARAMS
            echo "running task $REAL_TASK_ID with alpha=$alpha"
        """),
        directives=[
            "--partition=debug",
            "--time=00:01:00",
            "--mem=100M",
        ],
    )

    returncode, state_db, stdout, stderr = run_slurmigo(
        tmp_path,
        params_path,
        script_path,
        max_resubmits=3,
        timeout_multiplier=1.5,
    )

    assert os.path.exists(state_db), f"state.db not found at {state_db}"

    statuses = get_final_statuses(state_db)
    details = get_job_details(state_db)

    for task_id in [1, 2, 3]:
        assert task_id in statuses, f"Task {task_id} missing from state.db"
        assert statuses[task_id] == "COMPLETED", (
            f"Expected task {task_id} to be COMPLETED, got {statuses[task_id]}"
        )

    for task_id in [1, 2, 3]:
        assert details[task_id]["wall_time"] is not None, (
            f"Expected wall_time to be set for task {task_id}, got None"
        )
