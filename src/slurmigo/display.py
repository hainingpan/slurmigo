# slurmigo — display: DISPLAY stage (Rich TUI)

from __future__ import annotations

import shutil
import time

from rich.columns import Columns
from rich.console import Console, Group
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

console = Console()


class Display:
    """Rich TUI renderer for slurmigo job status."""

    def __init__(self, store, check_interval: int = 60):
        """Store a reference to the Store instance.

        Parameters
        ----------
        store : Store
            SQLite-backed store that provides job data.
        check_interval : int
            Seconds between refresh cycles (shown in footer).
        """
        self.store = store
        self.check_interval = check_interval

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def render(self, errors: list[str] | None = None, pending_reasons: dict[str, str] | None = None) -> Group:
        """Generate the full Rich display.

        Equivalent to the original ``generate_display(state)`` but reads
        data from ``self.store`` and adds an optional error panel.

        Returns a Rich ``Group`` containing all display elements.
        """
        self._pending_reasons = pending_reasons or {}
        terminal_size = shutil.get_terminal_size()
        terminal_width = terminal_size.columns
        terminal_height = terminal_size.lines
        # Use most of the terminal width for the grid (subtract panel borders)
        grid_width = terminal_width - 6  # Account for panel borders

        counts = self.store.get_counts()
        total = sum(counts.values())
        completed = counts.get("COMPLETED", 0)
        running = counts.get("RUNNING", 0)
        pending = counts.get("PENDING", 0)
        failed = counts.get("FAILED", 0) + counts.get("FAILED_PERMANENTLY", 0)
        not_submitted = counts.get("NOT_SUBMITTED", 0)
        pending_resub = counts.get("PENDING_RESUBMISSION", 0)

        # Calculate progress percentage
        progress_pct = (completed / total * 100) if total > 0 else 0

        # Build header
        header = Text(
            f"JOB MANAGER - {time.ctime()}", style="bold white", justify="center"
        )

        # Build progress bar text
        bar_width = 40
        filled = int(bar_width * progress_pct / 100)
        progress_bar = Text()
        progress_bar.append("Progress: [")
        progress_bar.append("█" * filled, style="green")
        progress_bar.append("░" * (bar_width - filled), style="dim")
        progress_bar.append(f"] {progress_pct:.1f}%")

        # Build combined stats (block + count + label)
        stats = Text()
        stats.append("█", style="green")
        stats.append(f" {completed} ", style="green bold")
        stats.append("Completed  ")
        stats.append("█", style="cyan bold")
        stats.append(f" {running} ", style="cyan bold")
        stats.append("Running  ")
        stats.append("█", style="yellow")
        stats.append(f" {pending} ", style="yellow bold")
        stats.append("Pending  ")
        stats.append("█", style="red")
        stats.append(f" {failed} ", style="red bold")
        stats.append("Failed  ")
        stats.append("░", style="dim")
        stats.append(f" {not_submitted} ", style="dim")
        stats.append("Not Submitted")
        if pending_resub > 0:
            stats.append("  ")
            stats.append("▒", style="yellow")
            stats.append(f" {pending_resub} ", style="yellow bold")
            stats.append("Resubmit")

        # Build combined progress/stats section (side by side, stats aligned right)
        stats_section = Table.grid(expand=True)
        stats_section.add_column(justify="left")
        stats_section.add_column(justify="right")
        stats_section.add_row(progress_bar, stats)

        # Calculate available height for the grid
        # Fixed elements: header(3) + stats(1) + blank(2) + panel_border(2)
        #   + tables(~12) + footer(1) + blank(1) = ~22 lines
        fixed_overhead = 22
        available_grid_rows = max(5, terminal_height - fixed_overhead)

        # Calculate total rows needed for full grid
        total_grid_rows = (total + grid_width - 1) // grid_width

        # Only apply max_rows constraint if grid would be too tall
        max_rows = (
            available_grid_rows if total_grid_rows > available_grid_rows else None
        )

        # Build the grid (with optional height constraint)
        grid = self._build_grid(grid_width, max_rows)

        # Combine all elements
        elements = [
            Panel(header, style="bold blue"),
            "",
            stats_section,
            "",
            Panel(grid, title="Job Status Grid", border_style="dim"),
            "",
        ]

        # Build tables and arrange them side by side
        running_table = self._build_running_table()
        pending_table = self._build_pending_table()
        completed_table = self._build_completed_table()
        failed_table = self._build_failed_table()

        # Collect tables that exist
        tables = []
        if running_table:
            tables.append(running_table)
        if pending_table:
            tables.append(pending_table)
        if completed_table:
            tables.append(completed_table)
        if failed_table:
            tables.append(failed_table)

        # Display tables side by side using Columns
        if tables:
            elements.append(Columns(tables, equal=False, expand=False))

        # Error panel
        error_panel = self._build_error_panel(errors)
        if error_panel is not None:
            elements.append("")
            elements.append(error_panel)

        elements.append("")
        elements.append(
            Text(
                f"Refreshing every {self.check_interval} seconds... (Ctrl+C to stop)",
                style="dim italic",
            )
        )

        return Group(*elements)

    # ------------------------------------------------------------------
    # Grid
    # ------------------------------------------------------------------

    def _build_grid(self, width: int, max_rows: int | None = None) -> Text:
        """Build a defrag-style colored grid showing job statuses.

        If *max_rows* is specified and the full grid exceeds it, zoom to
        show only the area around running jobs with ``...`` indicators.
        """
        # BUG FIX 3: Guard minimum width to prevent absurdly tall grids
        if width < 20:
            width = 20

        text = Text()
        jobs = self.store.get_all()  # Already sorted by task_id
        total_tasks = len(jobs)
        total_rows = (total_tasks + width - 1) // width  # Ceiling division

        # Determine if we need to zoom
        need_zoom = max_rows is not None and total_rows > max_rows and max_rows >= 3

        if need_zoom:
            # Find running job indices
            running_indices = []
            for i, job in enumerate(jobs):
                if job["status"] == "RUNNING":
                    running_indices.append(i)

            if running_indices:
                # Calculate the row range to display (centered on running jobs)
                min_running_row = min(running_indices) // width
                max_running_row = max(running_indices) // width

                # Try to show context around running jobs
                display_rows = max_rows - 2  # Reserve 2 rows for "..." indicators
                running_span = max_running_row - min_running_row + 1

                if running_span >= display_rows:
                    # Running jobs span too many rows, just show from min_running_row
                    start_row = min_running_row
                    end_row = min_running_row + display_rows - 1
                else:
                    # Center the display around running jobs
                    padding = (display_rows - running_span) // 2
                    start_row = max(0, min_running_row - padding)
                    end_row = min(total_rows - 1, start_row + display_rows - 1)
                    # Adjust if we hit the bottom
                    if end_row == total_rows - 1:
                        start_row = max(0, end_row - display_rows + 1)

                # Build the zoomed grid
                show_top_ellipsis = start_row > 0
                show_bottom_ellipsis = end_row < total_rows - 1

                if show_top_ellipsis:
                    # BUG FIX 1: Tasks are 1-indexed, not 0-indexed
                    text.append(
                        f"... ({start_row} rows above, tasks #1-{start_row * width})\n",
                        style="dim italic",
                    )

                # BUG FIX 2: Track whether bottom ellipsis was appended inline
                bottom_ellipsis_appended = False

                for i, job in enumerate(jobs):
                    row = i // width
                    if row < start_row:
                        continue
                    if row > end_row:
                        break

                    status = job["status"]
                    if status == "COMPLETED":
                        text.append("█", style="green")
                    elif status == "RUNNING":
                        text.append("█", style="cyan bold")
                    elif status == "PENDING":
                        text.append("█", style="yellow")
                    elif status in ["FAILED", "FAILED_PERMANENTLY"]:
                        text.append("█", style="red")
                    elif status == "PENDING_RESUBMISSION":
                        text.append("▒", style="yellow")
                    else:  # NOT_SUBMITTED
                        text.append("░", style="dim")

                    # Add newline at row boundary
                    is_row_end = (i + 1) % width == 0
                    is_last_visible_row = row == end_row
                    if is_row_end:
                        if is_last_visible_row and show_bottom_ellipsis:
                            # Append ellipsis inline instead of newline
                            remaining_rows = total_rows - end_row - 1
                            remaining_start_task = (end_row + 1) * width
                            text.append(
                                f" ... ({remaining_rows} rows below, tasks #{remaining_start_task}-{total_tasks - 1})",
                                style="dim italic",
                            )
                            bottom_ellipsis_appended = True
                        text.append("\n")

                # BUG FIX 2: If bottom ellipsis needed but last row was partial
                # (not a full row), the inline append never fires. Append it now.
                if show_bottom_ellipsis and not bottom_ellipsis_appended:
                    remaining_rows = total_rows - end_row - 1
                    remaining_start_task = (end_row + 1) * width
                    text.append(
                        f" ... ({remaining_rows} rows below, tasks #{remaining_start_task}-{total_tasks - 1})\n",
                        style="dim italic",
                    )

                return text

        # Full grid (no zooming needed)
        for i, job in enumerate(jobs):
            status = job["status"]
            if status == "COMPLETED":
                text.append("█", style="green")
            elif status == "RUNNING":
                text.append("█", style="cyan bold")
            elif status == "PENDING":
                text.append("█", style="yellow")
            elif status in ["FAILED", "FAILED_PERMANENTLY"]:
                text.append("█", style="red")
            elif status == "PENDING_RESUBMISSION":
                text.append("▒", style="yellow")
            else:  # NOT_SUBMITTED
                text.append("░", style="dim")

            # Add newline at row boundary
            if (i + 1) % width == 0:
                text.append("\n")

        return text

    # ------------------------------------------------------------------
    # Detail tables
    # ------------------------------------------------------------------

    def _build_running_table(self, limit: int = 10) -> Table | None:
        """Build a table of currently running jobs."""
        running_jobs = self.store.get_running()

        if not running_jobs:
            return None

        table = Table(
            title="Running Jobs",
            title_style="bold cyan",
            show_header=True,
            header_style="bold cyan",
        )
        table.add_column("Task", style="cyan", justify="right")
        table.add_column("Job ID", justify="right")
        table.add_column("Elapsed", style="green", justify="right")

        for job in running_jobs[:limit]:
            elapsed = job.get("elapsed_time") or "-"
            table.add_row(
                f"#{job['task_id']}",
                str(job["job_id"]),
                elapsed,
            )

        if len(running_jobs) > limit:
            table.add_row("...", f"+{len(running_jobs) - limit} more", "")

        return table

    def _build_pending_table(self, limit: int = 10) -> Table | None:
        """Build a table of pending jobs."""
        pending_jobs = self.store.get_pending()

        if not pending_jobs:
            return None

        table = Table(
            title="Pending Jobs",
            title_style="bold yellow",
            show_header=True,
            header_style="bold yellow",
        )
        table.add_column("Task", style="yellow", justify="right")
        table.add_column("Job ID", justify="right")
        table.add_column("Reason", style="dim", justify="left")
        table.add_column("Retries", justify="right")

        # Build a lookup of pending reasons from squeue
        # (slurm_details is not available here, so we read reason from job record)
        for job in pending_jobs[:limit]:
            job_id = str(job["job_id"]) if job.get("job_id") else "-"
            retries = str(max(0, (job.get("submit_count") or 1) - 1))
            # Get reason from slurm_details if available
            reason = self._pending_reasons.get(job_id, "") if hasattr(self, "_pending_reasons") else ""
            table.add_row(f"#{job['task_id']}", job_id, reason, retries)

        if len(pending_jobs) > limit:
            table.add_row("...", f"+{len(pending_jobs) - limit} more", "", "")

        return table

    def _build_completed_table(self, limit: int = 10) -> Table | None:
        """Build a table of recently completed jobs (by task_id, highest first)."""
        completed_jobs = [
            job for job in self.store.get_completed() if job.get("wall_time")
        ]

        if not completed_jobs:
            return None

        # Sort by task_id descending to get "recent" completions
        completed_jobs.sort(key=lambda x: x["task_id"], reverse=True)

        table = Table(
            title="Recently Completed",
            title_style="bold green",
            show_header=True,
            header_style="bold green",
        )
        table.add_column("Task", style="green", justify="right")
        table.add_column("Job ID", justify="right")
        table.add_column("Wall Time", style="cyan", justify="right")

        for job in completed_jobs[:limit]:
            table.add_row(
                f"#{job['task_id']}",
                str(job["job_id"]),
                job.get("wall_time") or "-",
            )

        if len(completed_jobs) > limit:
            table.add_row("...", f"+{len(completed_jobs) - limit} more", "")

        return table

    def _build_failed_table(self, limit: int = 10) -> Table | None:
        """Build a table of failed jobs (FAILED and FAILED_PERMANENTLY)."""
        failed_jobs = self.store.get_failed()

        if not failed_jobs:
            return None

        # Sort by task_id descending to show recent failures first
        failed_jobs.sort(key=lambda x: x["task_id"], reverse=True)

        table = Table(
            title="Failed Jobs",
            title_style="bold red",
            show_header=True,
            header_style="bold red",
        )
        table.add_column("Task", style="red", justify="right")
        table.add_column("Job ID", style="red", justify="right")
        table.add_column("Status", justify="left")
        table.add_column("Reason", style="dim", justify="left")

        for job in failed_jobs[:limit]:
            status_short = "PERM" if job["status"] == "FAILED_PERMANENTLY" else "FAIL"
            reason = self._format_perm_reason(job)
            table.add_row(
                f"#{job['task_id']}",
                job.get("job_id") or "-",
                status_short,
                reason,
            )

        if len(failed_jobs) > limit:
            table.add_row("...", f"+{len(failed_jobs) - limit}", "")

        return table

    @staticmethod
    def _format_perm_reason(job: dict) -> str:
        raw = job.get("failure_reason") or "-"
        if job.get("status") != "FAILED_PERMANENTLY":
            return raw

        if "|max_retries(" in raw:
            base, rest = raw.split("|max_retries(", 1)
            count = rest.rstrip(")")
            return f"{base} ({count} retries)"

        if "|not_retryable" in raw:
            base = raw.split("|not_retryable", 1)[0]
            return f"{base} (not retryable)"

        return raw

    # ------------------------------------------------------------------
    # Error panel (NEW)
    # ------------------------------------------------------------------

    def _build_error_panel(self, errors: list[str] | None) -> Panel | None:
        """Build a panel showing recent errors.

        Returns ``None`` if *errors* is empty or ``None``.
        Shows up to the last 5 error messages.
        """
        if not errors:
            return None

        # Show up to last 5 error messages
        recent = errors[-5:]
        text = Text()
        for i, err in enumerate(recent):
            text.append(f"• {err}", style="red")
            if i < len(recent) - 1:
                text.append("\n")

        return Panel(
            text,
            title="[bold red]Recent Errors[/bold red]",
            border_style="red",
        )
