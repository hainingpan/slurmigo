# slurmigo — sense: SENSE stage (Slurm polling & script parsing)
#
# This is the ONLY module that calls subprocess / interacts with Slurm CLI.
# All functions return safe defaults when Slurm is unavailable.

from __future__ import annotations

import getpass
import logging
import os
import re
import subprocess
from collections import defaultdict

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Short-flag → long-name mapping for #SBATCH directives
# ---------------------------------------------------------------------------
_SHORT_FLAGS: dict[str, str] = {
    "-p": "partition",
    "-t": "time",
    "-J": "job-name",
    "-o": "output",
    "-e": "error",
    "-n": "ntasks",
    "-c": "cpus-per-task",
    "-N": "nodes",
}

# Regex for parsing #SBATCH lines:
#   Group 1: flag (e.g. --partition or -p)
#   Group 2: value after '=' or whitespace
_SBATCH_RE = re.compile(
    r"^\s*#SBATCH\s+"  # leading whitespace + #SBATCH
    r"(-{1,2}[\w-]+)"  # flag (short or long)
    r"(?:[=\s]\s*(.+))?\s*$"  # optional =value or space-separated value
)


def _get_user() -> str:
    """Return the current username, with getpass fallback."""
    return os.environ.get("USER") or getpass.getuser()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def get_job_details() -> dict[str, dict[str, str]]:
    """Poll ``squeue`` for job IDs, states, and elapsed times.

    Returns
    -------
    dict[str, dict]
        ``{"job_id": {"state": "RUNNING", "elapsed": "00:15:23"}, ...}``
        Empty dict on any error.
    """
    job_details: dict[str, dict[str, str]] = {}
    try:
        user = _get_user()
        cmd = f'squeue -u {user} -h -o "%A|%T|%M"'
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.strip().split("\n"):
            if line:
                parts = line.strip().split("|")
                if len(parts) >= 3:
                    job_id = parts[0].strip()
                    state = parts[1].strip()
                    elapsed = parts[2].strip()
                    job_details[job_id] = {"state": state, "elapsed": elapsed}
        return job_details
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Error getting Slurm job details: %s", e)
        return job_details


def get_queue_per_partition() -> dict[str, int]:
    """Poll ``squeue`` for per-partition job counts.

    Returns
    -------
    dict[str, int]
        ``{"main": 342, "gpu": 50}``  Empty dict on error.
    """
    partition_counts: dict[str, int] = defaultdict(int)
    try:
        user = _get_user()
        cmd = f'squeue -u {user} -h -o "%P"'
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        for line in result.stdout.strip().split("\n"):
            if line:
                partition_counts[line.strip()] += 1
        return dict(partition_counts)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Error getting Slurm queue: %s", e)
        return dict(partition_counts)


def get_final_status(job_id: str) -> tuple[str, str, str | None]:
    """Query ``sacct`` for the final state of a completed job.

    Handles the multi-line sacct output by preferring the line whose
    job ID matches *exactly* (no ``.batch`` / ``.extern`` suffix).

    Returns
    -------
    tuple[str, str, str | None]
        ``(state, reason, elapsed)``  e.g. ``("COMPLETED", "", "01:23:45")``.
        Returns ``("UNKNOWN", "", None)`` on any error.
    """
    try:
        cmd = f"sacct -j {job_id} --format=State,Reason,Elapsed -n -P"
        result = subprocess.run(
            cmd,
            shell=True,
            capture_output=True,
            text=True,
            check=True,
        )
        lines = [ln for ln in result.stdout.strip().split("\n") if ln.strip()]
        if not lines:
            return ("UNKNOWN", "", None)

        # sacct may return multiple lines (batch step, extern step, etc.).
        # Prefer the line whose JobID matches exactly (no .batch suffix).
        # We can't see the JobID column in our format string, but the main
        # job line is always first and the .batch/.extern lines follow.
        # Strategy: if >1 line, use the first line (the main job entry).
        chosen = lines[0]

        parts = chosen.split("|")
        final_state = parts[0].strip() if len(parts) > 0 else "UNKNOWN"
        reason = parts[1].strip() if len(parts) > 1 else ""
        elapsed = parts[2].strip() if len(parts) > 2 else None

        if not final_state:
            return ("UNKNOWN", reason, elapsed)
        return (final_state, reason, elapsed)
    except (subprocess.CalledProcessError, FileNotFoundError) as e:
        logger.error("Error getting job final status for %s: %s", job_id, e)
        return ("UNKNOWN", "", None)


def parse_sbatch_directives(script_path: str) -> dict[str, str]:
    """Parse ``#SBATCH`` directives from a submission script.

    Handles all common forms::

        #SBATCH --partition=main
        #SBATCH --partition main
        #SBATCH -p main
        #SBATCH -p=main

    Short flags are normalised to their long-form names (without ``--``).

    Returns
    -------
    dict[str, str]
        ``{"partition": "main", "time": "01:00:00", ...}``
        Empty dict when the script has no directives or on error.
    """
    directives: dict[str, str] = {}
    try:
        with open(script_path, "r") as fh:
            for line in fh:
                m = _SBATCH_RE.match(line)
                if m is None:
                    continue
                flag = m.group(1)
                raw_value = (m.group(2) or "").strip()
                # Strip inline comments: '--exclude=node01  # skip this' -> 'node01'
                value = re.sub(r"\s+#\s+.*$", "", raw_value).strip()

                if flag.startswith("--"):
                    # Long form: strip leading --
                    key = flag[2:]
                else:
                    # Short form: look up in mapping
                    key = _SHORT_FLAGS.get(flag, flag.lstrip("-"))

                directives[key] = value
    except FileNotFoundError:
        logger.error("Submission script not found: %s", script_path)
    except OSError as e:
        logger.error("Error reading submission script %s: %s", script_path, e)
    return directives
