import math
from typing import Dict, List, Optional, Tuple

JobRecord = Dict[str, object]


class ConfigLike:
    max_resubmits: int = 3
    max_jobs_per_partition: Dict[str, int] = {"default": 150}
    timeout_multiplier: float = 1.5
    oom_multiplier: float = 1.5


class StoreLike:
    def get_by_status(self, _status: str) -> List[JobRecord]:
        raise NotImplementedError

    def get_all(self) -> List[JobRecord]:
        raise NotImplementedError

    def update_status(self, _task_id: int, _status: str, **_kwargs: object) -> None:
        raise NotImplementedError


class Decide:
    config: ConfigLike
    store: StoreLike

    def __init__(self, config: ConfigLike, store: StoreLike) -> None:
        self.config = config
        self.store = store

    def update_job_statuses(
        self,
        slurm_details: Dict[str, Dict[str, Optional[str]]],
        final_statuses: Dict[str, Tuple[str, str, Optional[str]]],
    ) -> None:
        tracked_jobs = self.store.get_by_status("PENDING") + self.store.get_by_status(
            "RUNNING"
        )
        normalized_slurm: Dict[str, Dict[str, Optional[str]]] = {
            str(job_id): details for job_id, details in slurm_details.items()
        }
        normalized_final: Dict[str, Tuple[str, str, Optional[str]]] = {
            str(job_id): status for job_id, status in final_statuses.items()
        }

        for job in tracked_jobs:
            task_id = self._task_id(job)
            job_id = job.get("job_id")
            if job_id is None:
                continue

            job_id_str = str(job_id)
            if job_id_str in normalized_slurm:
                details = normalized_slurm[job_id_str]
                slurm_state = str(details.get("state") or "").upper()
                elapsed = details.get("elapsed")

                if slurm_state in ("RUNNING", "CONFIGURING", "COMPLETING"):
                    self.store.update_status(task_id, "RUNNING", elapsed_time=elapsed)
                elif slurm_state == "PENDING":
                    self.store.update_status(task_id, "PENDING", elapsed_time=None)
                else:
                    self.store.update_status(task_id, "PENDING", elapsed_time=None)
                continue

            if job_id_str not in normalized_final:
                continue

            final_state, reason, elapsed = normalized_final[job_id_str]
            final_state = str(final_state).upper()
            # sacct's Reason column is often useless ("None", empty).
            # The State column (TIMEOUT, OUT_OF_MEMORY, PREEMPTED) is the
            # actual reason.  Use state as the failure_reason when reason
            # is empty or uninformative.
            reason_value = reason or ""
            if not reason_value or reason_value.lower() in ("none", ""):
                reason_value = final_state

            if "COMPLETED" in final_state:
                self.store.update_status(
                    task_id,
                    "COMPLETED",
                    wall_time=elapsed,
                    elapsed_time=None,
                )
            elif final_state in ("PENDING", "REQUEUED", "SUSPENDED"):
                self.store.update_status(task_id, "PENDING", elapsed_time=None)
            elif final_state in ("RUNNING", "CONFIGURING", "COMPLETING"):
                self.store.update_status(task_id, "RUNNING", elapsed_time=elapsed)
            elif final_state == "UNKNOWN":
                continue
            else:
                self.store.update_status(
                    task_id,
                    "FAILED",
                    failure_reason=reason_value,
                    wall_time=elapsed,
                )

    def handle_failures(self) -> None:
        for job in self.store.get_by_status("FAILED"):
            task_id = self._task_id(job)
            submit_count = self._to_int(job.get("submit_count"), 0)
            reason = self._to_str(job.get("failure_reason"))
            reason_lower = reason.lower()
            reason_upper = reason.upper()

            if submit_count >= int(self.config.max_resubmits):
                self.store.update_status(
                    task_id,
                    "FAILED_PERMANENTLY",
                    failure_reason=f"{reason}|max_retries({submit_count})",
                )
                continue

            if "preempt" in reason_lower or reason_upper == "PREEMPTED":
                self.store.update_status(task_id, "PENDING_RESUBMISSION")
                continue

            if reason_upper == "TIMEOUT" or "timeout" in reason_lower:
                self.store.update_status(
                    task_id,
                    "PENDING_RESUBMISSION",
                    failure_reason="TIMEOUT|scale_time",
                )
                continue

            if (
                reason_upper == "OUT_OF_MEMORY"
                or "oom" in reason_lower
                or "memory" in reason_lower
            ):
                self.store.update_status(
                    task_id,
                    "PENDING_RESUBMISSION",
                    failure_reason="OUT_OF_MEMORY|scale_mem",
                )
                continue

            self.store.update_status(
                task_id,
                "FAILED_PERMANENTLY",
                failure_reason=f"{reason or 'unknown'}|not_retryable",
            )

    def get_jobs_to_submit(
        self, queue_counts: Dict[str, int], partition: str
    ) -> List[JobRecord]:
        partition_limits = self.config.max_jobs_per_partition
        max_for_partition = int(
            partition_limits.get(partition, partition_limits.get("default", 150))
        )
        available_slots = max_for_partition - int(queue_counts.get(partition, 0))
        if available_slots <= 0:
            return []

        pending_resub = self.store.get_by_status("PENDING_RESUBMISSION")
        with_scale: List[JobRecord] = []
        without_scale: List[JobRecord] = []
        for job in pending_resub:
            reason = self._to_str(job.get("failure_reason")).lower()
            if "scale_time" in reason or "scale_mem" in reason:
                with_scale.append(job)
            else:
                without_scale.append(job)

        with_scale = sorted(with_scale, key=self._task_id)
        without_scale = sorted(without_scale, key=self._task_id)
        not_submitted = sorted(
            self.store.get_by_status("NOT_SUBMITTED"), key=self._task_id
        )

        ordered = with_scale + without_scale + not_submitted
        return ordered[:available_slots]

    def calculate_scaled_resources(self, task_id: int) -> Dict[str, str]:
        job = self._get_job(task_id)
        if not job:
            return {}

        reason = self._to_str(job.get("failure_reason")).lower()
        status = self._to_str(job.get("status")) or "NOT_SUBMITTED"

        if "scale_time" in reason:
            base_time = self._to_optional_str(job.get("current_time_limit"))
            if not base_time:
                base_time = self._to_optional_str(job.get("original_time_limit"))
            if not base_time:
                return {}

            seconds = self._parse_time_to_seconds(base_time)
            new_seconds = max(
                1, int(math.ceil(seconds * float(self.config.timeout_multiplier)))
            )
            new_time = self._seconds_to_time(new_seconds)
            self.store.update_status(task_id, status, current_time_limit=new_time)
            return {"time": new_time}

        if "scale_mem" in reason:
            base_mem = self._to_optional_str(job.get("current_mem"))
            if not base_mem:
                base_mem = self._to_optional_str(job.get("original_mem"))
            if not base_mem:
                return {}

            mem_mb = self._parse_mem_to_mb(base_mem)
            new_mem_mb = max(
                1, int(math.ceil(mem_mb * float(self.config.oom_multiplier)))
            )
            new_mem = self._mb_to_mem(new_mem_mb)
            self.store.update_status(task_id, status, current_mem=new_mem)
            return {"mem": new_mem}

        return {}

    def _get_job(self, task_id: int) -> Optional[JobRecord]:
        for job in self.store.get_all():
            if self._task_id(job) == int(task_id):
                return job
        return None

    def _task_id(self, job: JobRecord) -> int:
        return self._to_int(job.get("task_id"), 0)

    def _to_int(self, value: object, default: int) -> int:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            text = value.strip()
            if text:
                return int(text)
        return default

    def _to_str(self, value: object) -> str:
        if value is None:
            return ""
        if isinstance(value, str):
            return value
        return str(value)

    def _to_optional_str(self, value: object) -> Optional[str]:
        if value is None:
            return None
        if isinstance(value, str):
            return value if value.strip() else None
        text = str(value)
        return text if text.strip() else None

    def _parse_time_to_seconds(self, time_str: str) -> int:
        value = (time_str or "").strip()
        if not value:
            return 0

        if "-" in value:
            days_str, rest = value.split("-", 1)
            days = int(days_str)
            hms = rest.split(":")
            if len(hms) != 3:
                return 0
            hours, minutes, seconds = (int(part) for part in hms)
            return days * 86400 + hours * 3600 + minutes * 60 + seconds

        parts = value.split(":")
        if len(parts) == 3:
            hours, minutes, seconds = (int(part) for part in parts)
            return hours * 3600 + minutes * 60 + seconds
        if len(parts) == 2:
            minutes, seconds = (int(part) for part in parts)
            return minutes * 60 + seconds
        return 0

    def _seconds_to_time(self, seconds: int) -> str:
        total = max(0, int(seconds))
        days, rem = divmod(total, 86400)
        hours, rem = divmod(rem, 3600)
        minutes, secs = divmod(rem, 60)

        if days > 0:
            return f"{days}-{hours:02d}:{minutes:02d}:{secs:02d}"
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"

    def _parse_mem_to_mb(self, mem_str: str) -> int:
        value = (mem_str or "").strip().upper()
        if not value:
            return 0

        if value.endswith("G"):
            return int(float(value[:-1]) * 1024)
        if value.endswith("M"):
            return int(float(value[:-1]))
        return int(float(value))

    def _mb_to_mem(self, mb: int) -> str:
        amount = max(0, int(mb))
        if amount >= 1024:
            return f"{int(math.ceil(amount / 1024))}G"
        return f"{amount}M"
