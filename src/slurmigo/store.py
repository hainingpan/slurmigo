# slurmigo — store: SQLite CRUD layer for job records

from __future__ import annotations

import json
import os
import sqlite3


class Store:
    """SQLite-backed storage for slurmigo job records."""

    def __init__(self, db_path: str):
        """Open (or create) the SQLite database at *db_path*.

        Creates parent directories if needed, enables WAL mode,
        and ensures the ``jobs`` table exists.
        """
        parent = os.path.dirname(db_path)
        if parent:
            os.makedirs(parent, exist_ok=True)

        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._create_table()

    # ------------------------------------------------------------------
    # Schema
    # ------------------------------------------------------------------

    def _create_table(self) -> None:
        with self._conn:
            self._conn.execute(
                """
                CREATE TABLE IF NOT EXISTS jobs (
                    task_id            INTEGER PRIMARY KEY,
                    params             TEXT    NOT NULL,
                    status             TEXT    NOT NULL DEFAULT 'NOT_SUBMITTED',
                    job_id             TEXT,
                    submit_count       INTEGER DEFAULT 0,
                    failure_reason     TEXT,
                    elapsed_time       TEXT,
                    wall_time          TEXT,
                    original_time_limit TEXT,
                    original_mem       TEXT,
                    current_time_limit TEXT,
                    current_mem        TEXT
                )
                """
            )

    # ------------------------------------------------------------------
    # Bulk initialisation
    # ------------------------------------------------------------------

    def initialize(self, params_lines: list[str]) -> None:
        """Bulk-insert tasks from a list of parameter strings.

        ``task_id`` is the 1-indexed position in the list.
        If the table already contains rows the call is a no-op (idempotent).
        """
        row = self._conn.execute("SELECT COUNT(*) FROM jobs").fetchone()
        if row[0] > 0:
            return

        with self._conn:
            self._conn.executemany(
                "INSERT INTO jobs (task_id, params) VALUES (?, ?)",
                [(i + 1, line.strip()) for i, line in enumerate(params_lines)],
            )

    # ------------------------------------------------------------------
    # Reads
    # ------------------------------------------------------------------

    def get_all(self) -> list[dict[str, object]]:
        """Return every job row as a list of dicts."""
        rows = self._conn.execute("SELECT * FROM jobs ORDER BY task_id").fetchall()
        return [dict(r) for r in rows]

    def get_by_status(self, status: str) -> list[dict[str, object]]:
        """Return rows whose status matches *status*."""
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE status = ? ORDER BY task_id",
            (status,),
        ).fetchall()
        return [dict(r) for r in rows]

    def get_counts(self) -> dict[str, int]:
        """Return ``{status: count}`` for every status present in the table."""
        rows = self._conn.execute(
            "SELECT status, COUNT(*) AS cnt FROM jobs GROUP BY status"
        ).fetchall()
        return {r["status"]: r["cnt"] for r in rows}

    def get_running(self) -> list[dict[str, object]]:
        """Shorthand for ``get_by_status('RUNNING')``."""
        return self.get_by_status("RUNNING")

    def get_pending(self) -> list[dict[str, object]]:
        """Shorthand for ``get_by_status('PENDING')``."""
        return self.get_by_status("PENDING")

    def get_completed(self) -> list[dict[str, object]]:
        """Shorthand for ``get_by_status('COMPLETED')``."""
        return self.get_by_status("COMPLETED")

    def get_failed(self) -> list[dict[str, object]]:
        """Return rows with status FAILED or FAILED_PERMANENTLY."""
        rows = self._conn.execute(
            "SELECT * FROM jobs WHERE status IN ('FAILED', 'FAILED_PERMANENTLY') "
            "ORDER BY task_id"
        ).fetchall()
        return [dict(r) for r in rows]

    def is_all_finished(self) -> bool:
        """True when every job is COMPLETED or FAILED_PERMANENTLY."""
        row = self._conn.execute(
            "SELECT COUNT(*) FROM jobs WHERE status NOT IN ('COMPLETED', 'FAILED_PERMANENTLY')"
        ).fetchone()
        return row[0] == 0

    # ------------------------------------------------------------------
    # Writes
    # ------------------------------------------------------------------

    def update_status(self, task_id: int, status: str, **kwargs) -> None:
        """Set *status* (and any extra column values) for *task_id*."""
        fields = {"status": status}
        fields.update(kwargs)

        set_clause = ", ".join(f"{k} = ?" for k in fields)
        values = list(fields.values()) + [task_id]

        with self._conn:
            self._conn.execute(
                f"UPDATE jobs SET {set_clause} WHERE task_id = ?",
                values,
            )

    # ------------------------------------------------------------------
    # Migration helper
    # ------------------------------------------------------------------

    def import_from_json(self, json_path: str) -> None:
        """Populate the database from an existing ``job_manager_state.json``.

        Reads the JSON file, maps each entry to a row, and inserts them.
        Existing rows are skipped (uses INSERT OR IGNORE).
        """
        with open(json_path, "r") as f:
            state = json.load(f)

        with self._conn:
            for key, info in state.items():
                task_id = int(key)
                self._conn.execute(
                    """
                    INSERT OR IGNORE INTO jobs
                        (task_id, params, status, job_id, submit_count,
                         failure_reason, elapsed_time, wall_time,
                         original_time_limit, original_mem,
                         current_time_limit, current_mem)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        task_id,
                        info.get("params", ""),
                        info.get("status", "NOT_SUBMITTED"),
                        info.get("job_id"),
                        info.get("submit_count", 0),
                        info.get("failure_reason"),
                        info.get("elapsed_time"),
                        info.get("wall_time"),
                        info.get("original_time_limit"),
                        info.get("original_mem"),
                        info.get("current_time_limit"),
                        info.get("current_mem"),
                    ),
                )
