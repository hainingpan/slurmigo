from __future__ import annotations

import argparse
import importlib
import logging
import os
import sys
import time
from typing import Any, cast

from . import __version__, act, sense
from .decide import Decide
from .display import Display
from .store import Store


class Config:
    def __init__(
        self,
        params_file,
        script,
        state_dir=".slurmigo",
        check_interval=60,
        max_resubmits=3,
        timeout_multiplier=1.5,
        oom_multiplier=1.5,
        max_jobs_per_partition=None,
    ):
        self.params_file = params_file
        self.script = script
        self.state_dir = state_dir
        self.check_interval = check_interval
        self.max_resubmits = max_resubmits
        self.timeout_multiplier = timeout_multiplier
        self.oom_multiplier = oom_multiplier
        self.max_jobs_per_partition = max_jobs_per_partition or {"default": 150}


def _exit_config_error(message):
    print(f"Configuration error: {message}", file=sys.stderr)
    sys.exit(1)


def _parse_int(name, value, minimum=None, gt_zero=False):
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        if gt_zero:
            _exit_config_error(f"{name} must be an integer > 0")
        _exit_config_error(f"{name} must be an integer >= {minimum}")
        raise ValueError

    if gt_zero and parsed <= 0:
        _exit_config_error(f"{name} must be > 0")
    if minimum is not None and parsed < minimum:
        _exit_config_error(f"{name} must be >= {minimum}")
    return parsed


def _parse_float(name, value, minimum=None):
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        _exit_config_error(f"{name} must be a number >= {minimum}")
        raise ValueError

    if minimum is not None and parsed < minimum:
        _exit_config_error(f"{name} must be >= {minimum}")
    return parsed


def _load_toml_file(path):
    try:
        toml_module = importlib.import_module("tomllib")
    except ModuleNotFoundError:
        try:
            toml_module = importlib.import_module("tomli")
        except ModuleNotFoundError:
            _exit_config_error(
                "TOML support not available (install tomli for Python < 3.11)"
            )
            raise ModuleNotFoundError

    loaded = {}
    try:
        with open(path, "rb") as f:
            loaded = toml_module.load(f)
    except OSError as exc:
        _exit_config_error(f"unable to read config file '{path}': {exc}")
    except Exception as exc:
        _exit_config_error(f"unable to parse config file '{path}': {exc}")

    if not isinstance(loaded, dict):
        _exit_config_error("config file root must be a TOML table")
    return loaded


def load_config(path: str, overrides: dict[str, object]) -> Config:
    merged: dict[str, object] = {
        "state_dir": ".slurmigo",
        "check_interval": 60,
        "max_resubmits": 3,
        "timeout_multiplier": 1.5,
        "oom_multiplier": 1.5,
        "max_jobs_per_partition": {"default": 150},
    }

    if os.path.exists(path):
        merged.update(_load_toml_file(path))

    for key, value in overrides.items():
        if value is not None:
            merged[key] = value

    params_file = merged.get("params_file")
    script = merged.get("script")
    if not params_file:
        _exit_config_error("params_file is required (set in TOML or --params)")
    if not script:
        _exit_config_error("script is required (set in TOML or --script)")

    check_interval = _parse_int(
        "check_interval", merged.get("check_interval"), gt_zero=True
    )
    max_resubmits = _parse_int("max_resubmits", merged.get("max_resubmits"), minimum=0)
    timeout_multiplier = _parse_float(
        "timeout_multiplier",
        merged.get("timeout_multiplier"),
        minimum=1.0,
    )
    oom_multiplier = _parse_float(
        "oom_multiplier",
        merged.get("oom_multiplier"),
        minimum=1.0,
    )

    max_jobs = merged.get("max_jobs_per_partition", {"default": 150})
    if not isinstance(max_jobs, dict):
        _exit_config_error("max_jobs_per_partition must be a table/dict")
    max_jobs_dict = cast(dict[Any, Any], max_jobs)

    parsed_limits = {}
    for partition, value in max_jobs_dict.items():
        parsed_limits[str(partition)] = _parse_int(
            f"max_jobs_per_partition.{partition}",
            value,
            minimum=0,
        )

    return Config(
        params_file=str(params_file),
        script=str(script),
        state_dir=str(merged.get("state_dir", ".slurmigo")),
        check_interval=check_interval,
        max_resubmits=max_resubmits,
        timeout_multiplier=timeout_multiplier,
        oom_multiplier=oom_multiplier,
        max_jobs_per_partition=parsed_limits,
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="slurmigo")
    parser.add_argument("-p", "--params", dest="params_file")
    parser.add_argument("-s", "--script")
    parser.add_argument("-c", "--config", default="slurmigo.toml")
    parser.add_argument("--interval", type=int, dest="check_interval")
    parser.add_argument("--max-resubmits", type=int)
    parser.add_argument("--max-jobs", type=int)
    parser.add_argument(
        "--version", action="version", version=f"slurmigo {__version__}"
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    overrides = {}
    if args.params_file is not None:
        overrides["params_file"] = args.params_file
    if args.script is not None:
        overrides["script"] = args.script
    if args.check_interval is not None:
        overrides["check_interval"] = args.check_interval
    if args.max_resubmits is not None:
        overrides["max_resubmits"] = args.max_resubmits
    if args.max_jobs is not None:
        overrides["max_jobs_per_partition"] = {"default": args.max_jobs}

    config = load_config(args.config, overrides)

    if not os.path.exists(config.params_file):
        print(f"Error: params file not found: {config.params_file}", file=sys.stderr)
        sys.exit(1)
    if not os.path.exists(config.script):
        print(f"Error: submission script not found: {config.script}", file=sys.stderr)
        sys.exit(1)

    if args.dry_run:
        print("slurmigo dry-run configuration:")
        print(f"  params_file: {config.params_file}")
        print(f"  script: {config.script}")
        print(f"  state_dir: {config.state_dir}")
        print(f"  check_interval: {config.check_interval}")
        print(f"  max_resubmits: {config.max_resubmits}")
        print(f"  timeout_multiplier: {config.timeout_multiplier}")
        print(f"  oom_multiplier: {config.oom_multiplier}")
        print(f"  max_jobs_per_partition: {config.max_jobs_per_partition}")
        return

    rich_console = importlib.import_module("rich.console")
    rich_live = importlib.import_module("rich.live")
    Console = rich_console.Console
    Live = rich_live.Live

    os.makedirs(os.path.join(config.state_dir, "wrappers"), exist_ok=True)

    logging.basicConfig(
        filename=os.path.join(config.state_dir, "slurmigo.log"),
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    store = Store(os.path.join(config.state_dir, "state.db"))
    params, fmt = act.parse_params_file(config.params_file)
    _ = fmt
    store.initialize([str(p) for p in params])

    directives = sense.parse_sbatch_directives(config.script)
    partition = directives.get("partition", "default")
    original_time = directives.get("time")
    original_mem = directives.get("mem")
    if original_time or original_mem:
        for job in store.get_by_status("NOT_SUBMITTED"):
            store.update_status(
                _parse_int("task_id", job.get("task_id"), minimum=1),
                "NOT_SUBMITTED",
                original_time_limit=original_time,
                original_mem=original_mem,
                current_time_limit=original_time,
                current_mem=original_mem,
            )

    decide_engine = Decide(cast(Any, config), cast(Any, store))
    display_engine = Display(store)
    display_engine.check_interval = config.check_interval

    console = Console()
    recent_errors = []

    try:
        with Live(
            display_engine.render(errors=recent_errors),
            console=console,
            refresh_per_second=0.5,
            screen=True,
        ) as live:
            while True:
                try:
                    slurm_details = sense.get_job_details()

                    tracked_ids = {
                        str(j["job_id"])
                        for j in store.get_by_status("PENDING")
                        + store.get_by_status("RUNNING")
                        if j.get("job_id")
                    }
                    in_queue = set(slurm_details.keys())
                    missing_ids = tracked_ids - in_queue
                    final_statuses = {
                        job_id: sense.get_final_status(job_id) for job_id in missing_ids
                    }

                    decide_engine.update_job_statuses(
                        cast(dict[str, dict[str, str | None]], slurm_details),
                        final_statuses,
                    )
                    decide_engine.handle_failures()

                    queue_counts = sense.get_queue_per_partition()
                    jobs_to_submit = decide_engine.get_jobs_to_submit(
                        queue_counts, partition
                    )
                    for job in jobs_to_submit:
                        task_id = _parse_int("task_id", job.get("task_id"), minimum=1)
                        param_dict = (
                            params[task_id - 1] if task_id <= len(params) else {}
                        )
                        resource_overrides = decide_engine.calculate_scaled_resources(
                            task_id
                        )
                        sbatch_args = act.build_sbatch_args(
                            directives, resource_overrides
                        )
                        wrapper = act.generate_wrapper(
                            task_id,
                            param_dict,
                            config.script,
                            config.state_dir,
                        )
                        job_id = act.submit_job(wrapper, sbatch_args)
                        if job_id:
                            submit_count = (
                                _parse_int(
                                    "submit_count",
                                    job.get("submit_count", 0) or 0,
                                    minimum=0,
                                )
                                + 1
                            )
                            store.update_status(
                                task_id,
                                "PENDING",
                                job_id=job_id,
                                submit_count=submit_count,
                            )
                        else:
                            logging.error("Failed to submit task %d", task_id)
                            recent_errors.append(f"Submit failed for task {task_id}")

                except Exception as exc:
                    logging.error("Error in cycle: %s", exc, exc_info=True)
                    recent_errors.append(str(exc))
                    recent_errors = recent_errors[-10:]

                live.update(display_engine.render(errors=recent_errors[-5:]))

                if store.is_all_finished():
                    live.update(display_engine.render(errors=recent_errors[-5:]))
                    time.sleep(2)
                    break

                time.sleep(config.check_interval)

    except KeyboardInterrupt:
        logging.info("Stopped by user")

    console.print("\n[bold green]Done.[/bold green]")
