"""
Pipeline Manager — runs pipeline operations in background threads.

Manages batch pipeline, import, and monitor mode lifecycle
so the web server stays responsive.
"""

import json
import sys
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config import load_config, reset_config
from src.core.logger import get_logger

logger = get_logger(__name__)


class PipelineStatus:
    """Shared status object for pipeline operations."""

    def __init__(self):
        self.running = False
        self.mode: Optional[str] = None  # 'batch', 'import', 'monitor'
        self.progress = ""
        self.started_at: Optional[str] = None
        self.finished_at: Optional[str] = None
        self.last_result: Optional[dict] = None
        self.error: Optional[str] = None
        self._lock = threading.Lock()

    def start(self, mode: str):
        with self._lock:
            self.running = True
            self.mode = mode
            self.progress = "Starting..."
            self.started_at = datetime.now(timezone.utc).isoformat()
            self.finished_at = None
            self.last_result = None
            self.error = None

    def update(self, msg: str):
        with self._lock:
            self.progress = msg

    def finish(self, result: Optional[dict] = None):
        with self._lock:
            self.running = False
            self.progress = "Done"
            self.finished_at = datetime.now(timezone.utc).isoformat()
            self.last_result = result

    def fail(self, error: str):
        with self._lock:
            self.running = False
            self.progress = "Failed"
            self.finished_at = datetime.now(timezone.utc).isoformat()
            self.error = error

    @property
    def status(self) -> dict:
        with self._lock:
            return {
                "running": self.running,
                "mode": self.mode,
                "progress": self.progress,
                "started_at": self.started_at,
                "finished_at": self.finished_at,
                "last_result": self.last_result,
                "error": self.error,
            }


# ── Singleton ────────────────────────────────────────

_pipeline_status = PipelineStatus()
_batch_thread: Optional[threading.Thread] = None
_stop_batch = threading.Event()
_monitor_thread: Optional[threading.Thread] = None
_stop_monitor = threading.Event()


def get_pipeline_status() -> dict:
    return _pipeline_status.status


def run_batch_pipeline(config_path: str = "config/config.yaml", input_dirs: Optional[list] = None):
    """Run full batch pipeline in background thread."""
    global _batch_thread, _stop_batch
    if _pipeline_status.running:
        return {"error": "Pipeline already running"}

    _stop_batch.clear()

    def _run():
        _pipeline_status.start("batch")
        try:
            config = load_config(config_path)
            from src.orchestrator import SRFOrchestrator
            orch = SRFOrchestrator(config)

            _pipeline_status.update("Setting up directories...")
            orch.setup_directories()

            _pipeline_status.update("Running preprocessor...")
            if input_dirs:
                orch.run_preprocessor([Path(d) for d in input_dirs])
            else:
                orch.run_preprocessor()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update("Running grouper (merge)...")
            grouper_result = orch.run_grouper()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update("Running classifier...")
            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return
            orch.run_classifier()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update("Generating visualizations...")
            orch.run_visualizer()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update("Generating reports...")
            orch.run_reporter()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update("Sending emails...")
            email_results = orch.run_email_sender()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update("Importing to DB...")
            db_count = orch.import_to_db()

            result = {
                "mode": "batch",
                "db_imported": db_count,
                "emails_sent": sum(1 for r in email_results if r.get("success")),
                "grouper": grouper_result,
            }
            _pipeline_status.finish(result)

        except Exception as e:
            logger.exception("Batch pipeline failed")
            _pipeline_status.fail(str(e))

    _batch_thread = threading.Thread(target=_run, daemon=True, name="pipeline-batch")
    _batch_thread.start()
    return {"ok": True, "message": "Batch pipeline started"}


def run_import_only(config_path: str = "config/config.yaml"):
    """Run import only (from data/merged to DB)."""
    if _pipeline_status.running:
        return {"error": "Pipeline already running"}

    def _run():
        _pipeline_status.start("import")
        try:
            config = load_config(config_path)
            project_root = Path(config_path).resolve().parent.parent
            # config.paths.merged_dir 은 "./data/merged"
            merged_dir = (project_root / config.paths.merged_dir).resolve()

            _pipeline_status.update(f"Scanning {merged_dir}...")
            if not merged_dir.exists():
                _pipeline_status.fail(f"Merged directory not found: {merged_dir}")
                return

            from src.import_job import run_import as do_import
            result = do_import(merged_dir, config_path)
            _pipeline_status.finish(result)
        except Exception as e:
            logger.exception("Import failed")
            _pipeline_status.fail(str(e))

    thread = threading.Thread(target=_run, daemon=True, name="pipeline-import")
    thread.start()
    return {"ok": True, "message": "Import started"}


def start_monitor(config_path: str = "config/config.yaml"):
    """Start continuous folder monitoring in background."""
    global _monitor_thread, _stop_monitor

    if _pipeline_status.running:
        return {"error": "Pipeline already running"}
    if _monitor_thread and _monitor_thread.is_alive():
        return {"error": "Monitor already running"}

    _stop_monitor.clear()

    def _run():
        _pipeline_status.start("monitor")
        try:
            config = load_config(config_path)
            from src.orchestrator import SRFOrchestrator
            orch = SRFOrchestrator(config)
            orch.setup_directories()

            # Check watch folders exist
            for wf in config.paths.watch_folders:
                resolved = Path(wf)
                if not resolved.exists():
                    resolved.mkdir(parents=True, exist_ok=True)
                    _pipeline_status.update(f"Created watch folder: {resolved}")

            # Monitor loop
            processed_csv = set()        # all csv files processed (skip on re-detection)
            last_pipeline_parquet = {}   # scope_idx -> set of parquet names at last pipeline run
            processed_merged = set()     # merged file names that have been emailed + imported
            imported_events = 0
            cycle_count = 0

            # ── Wait timeout tracking ─────────────────────────────
            wait_start: float | None = None       # time.time() when wait for all-3 started
            pending_new_scope_files: dict = {}     # scope_idx -> set of pending parquet filenames

            def do_cleanup_on_timeout() -> bool:
                """Clean up partial scope files. Returns True if any file was cleaned."""
                cleaned = 0
                for i in range(1, 4):
                    for fname in pending_new_scope_files.get(i, set()):
                        fpath = config.paths.processed_dir / f"scope{i}" / fname
                        try:
                            fpath.unlink()
                            cleaned += 1
                        except Exception as e:
                            logger.warning(f"Cleanup failed: {fpath.name}: {e}")
                # Reset tracking
                nonlocal wait_start
                wait_start = None
                pending_new_scope_files.clear()
                for i in range(1, 4):
                    sd = config.paths.processed_dir / f"scope{i}"
                    last_pipeline_parquet[i] = set(
                        f.name for f in sd.glob("*.parquet")
                    ) if sd.exists() else set()
                return cleaned > 0

            # ── Initial snapshot ──────────────────────────────────
            # 1) Collect ALL existing CSV files in watch folders → mark as "already tracked"
            #    (so the monitor only reacts to NEWLY CREATED files after startup)
            existing_csv_count = 0
            for i, wf in enumerate(config.paths.watch_folders, 1):
                wf_path = Path(wf)
                if wf_path.exists():
                    for f in wf_path.glob("*.csv"):
                        if not f.name.endswith(':Zone.Identifier'):
                            processed_csv.add(f.name)
                            existing_csv_count += 1

            # 2) Snapshot existing scope parquet files
            for i in range(1, 4):
                sd = config.paths.processed_dir / f"scope{i}"
                last_pipeline_parquet[i] = set(
                    f.name for f in sd.glob("*.parquet")
                ) if sd.exists() else set()

            # 3) Snapshot already-processed merged files
            merged_dir = config.paths.merged_dir
            processed_merged.update(
                f.name for f in merged_dir.glob("*.parquet")
            )

            _pipeline_status.update(
                f"Initial: {existing_csv_count} existing CSV ignored, "
                f"{sum(len(v) for v in last_pipeline_parquet.values())} scope parquet, "
                f"{len(processed_merged)} merged tracked. "
                f"Waiting for NEW CSV files..."
            )

            while not _stop_monitor.is_set():
                # Step 1: Find and process NEW CSV files (skip if parquet already exists)
                newly_processed = False
                for i, wf in enumerate(config.paths.watch_folders, 1):
                    wf_path = Path(wf)
                    if not wf_path.exists():
                        continue

                    scope_dir = config.paths.processed_dir / f"scope{i}"
                    scope_dir.mkdir(parents=True, exist_ok=True)

                    csv_files = [f for f in wf_path.glob("*.csv")
                                 if not f.name.endswith(':Zone.Identifier')]
                    for csv_path in csv_files:
                        # Skip if already tracked or parquet already exists
                        pqt_path = scope_dir / f"{csv_path.stem}.parquet"
                        if csv_path.name in processed_csv or pqt_path.exists():
                            if csv_path.name not in processed_csv:
                                processed_csv.add(csv_path.name)
                            continue

                        _pipeline_status.update(f"Processing {csv_path.name}... ({int(csv_path.stat().st_size / 1024)}KB)")
                        try:
                            orch.preprocessor.process_one(csv_path, pqt_path, max_retries=1)
                            processed_csv.add(csv_path.name)
                            newly_processed = True
                        except Exception as e:
                            logger.warning(f"Failed {csv_path.name}: {e}")
                            _pipeline_status.update(f"Failed {csv_path.name}: {e}")

                # ── Step 2: Check if all 3 scopes are ready ────────
                wait_timeout = config.grouper.wait_timeout

                # Compute current new files per scope
                current_new = {}
                all_ready = True
                for i in range(1, 4):
                    sd = config.paths.processed_dir / f"scope{i}"
                    current = set(f.name for f in sd.glob("*.parquet")) if sd.exists() else set()
                    new_files = current - last_pipeline_parquet.get(i, set())
                    current_new[i] = new_files
                    if not new_files:
                        all_ready = False

                # ── Handle waiting state (timeout tracking) ────────
                if newly_processed and not all_ready:
                    # New files detected but not all scopes present
                    if wait_start is None:
                        wait_start = time.time()
                        pending_new_scope_files = dict(current_new)
                    else:
                        # Update pending files (more may have arrived during wait)
                        for i in range(1, 4):
                            if current_new[i]:
                                pending_new_scope_files[i] = current_new[i]

                    elapsed = time.time() - wait_start
                    remaining = max(0, wait_timeout - int(elapsed))
                    _pipeline_status.update(
                        f"Waiting for all 3 scopes... "
                        f"have: {[i for i in range(1, 4) if current_new[i]]}, "
                        f"missing: {[i for i in range(1, 4) if not current_new[i]]}, "
                        f"timeout in {remaining}s"
                    )

                    if elapsed >= wait_timeout:
                        _pipeline_status.update(
                            f"Timeout ({wait_timeout}s) — cleaning up partial scope files..."
                        )
                        do_cleanup_on_timeout()
                        _pipeline_status.update(
                            f"Timeout — cleaned up partial files. Waiting for fresh data..."
                        )
                    # Continue loop (wait for more files or next cycle)
                    time.sleep(config.system.check_interval)
                    continue

                elif not newly_processed and wait_start is not None:
                    # Still waiting but no new files this cycle — check timeout
                    elapsed = time.time() - wait_start
                    if elapsed >= wait_timeout:
                        _pipeline_status.update(
                            f"Timeout ({wait_timeout}s) — cleaning up partial scope files..."
                        )
                        do_cleanup_on_timeout()
                        _pipeline_status.update(
                            f"Timeout — cleaned up partial files. Waiting for fresh data..."
                        )
                    time.sleep(config.system.check_interval)
                    continue

                # ── Step 3: Pipeline execution ─────────────────────
                # At this point: either newly_processed=True AND all_ready=True
                #                 OR nothing happened (sleep below handles that)
                if not newly_processed and wait_start is None:
                    # Nothing new, not waiting — just poll
                    time.sleep(config.system.check_interval)
                    continue

                if not all_ready:
                    # Should not reach here (above blocks handle non-ready cases)
                    time.sleep(config.system.check_interval)
                    continue

                # ── Capture NEW scope files before pipeline (for cleanup) ──
                new_scope_files = {}
                for i in range(1, 4):
                    sd = config.paths.processed_dir / f"scope{i}"
                    current = set(f for f in sd.glob("*.parquet")) if sd.exists() else set()
                    old = last_pipeline_parquet.get(i, set())
                    new_scope_files[i] = [f for f in current if f.name not in old]

                # ── Run pipeline cycle ──
                cycle_count += 1
                _pipeline_status.update(f"[Cycle {cycle_count}] Running pipeline...")
                try:
                    for step_name, step_fn in [
                        ("Grouper (merge)", orch.run_grouper),
                        ("Classifier", orch.run_classifier),
                        ("Visualizer", orch.run_visualizer),
                        ("Reporter", orch.run_reporter),
                    ]:
                        _pipeline_status.update(f"[Cycle {cycle_count}] {step_name}...")
                        step_fn()

                    # Email: only for NEW merged files
                    if orch.email_sender:
                        current_merged = set(f.name for f in merged_dir.glob("*.parquet"))
                        new_merged = [f for f in merged_dir.glob("*.parquet")
                                      if f.name not in processed_merged]
                        if new_merged:
                            emailed = 0
                            import json
                            web_url = f"http://141.223.105.230:{config.web.port}"
                            for mf in new_merged:
                                cls_file = config.paths.results_dir / f"{mf.stem}_classification.json"
                                report_file = config.paths.reports_dir / f"{mf.stem}_report.md"
                                if cls_file.exists():
                                    summary = "Unclassified"
                                    try:
                                        summary = json.loads(cls_file.read_text()).get('description', summary)
                                    except Exception:
                                        pass
                                    try:
                                        event_id = mf.stem.replace('event_', '')
                                        content = report_file.read_text(encoding='utf-8') if report_file.exists() else "No report"
                                        content += f"\n\n🔗 **View in WebDB:** {web_url}/events/{event_id}\n"
                                        # Wrap email sending in try-except to prevent pipeline crash
                                        try:
                                            orch.email_sender.send_report(
                                                report_content=content,
                                                report_format='markdown',
                                                graph_files=[],
                                                classification_summary=summary,
                                            )
                                            emailed += 1
                                        except Exception as e:
                                            logger.warning(f"Email failed for {mf.name}: {e}")
                                    except Exception as e:
                                        logger.warning(f"Error preparing email for {mf.name}: {e}")
                            _pipeline_status.update(f"[Cycle {cycle_count}] Emails sent: {emailed}")
                            processed_merged.update(f.name for f in new_merged)
                        else:
                            _pipeline_status.update(f"[Cycle {cycle_count}] No new merged files")
                    else:
                        _pipeline_status.update(f"[Cycle {cycle_count}] Email sender disabled")

                    # Import to DB
                    _pipeline_status.update(f"[Cycle {cycle_count}] Importing to DB...")
                    db_count = orch.import_to_db()
                    imported_events += db_count

                    # Cleanup: delete NEW scope parquet files to prevent re-merge
                    cleaned = 0
                    for i in range(1, 4):
                        for f in new_scope_files.get(i, []):
                            try:
                                f.unlink()
                                cleaned += 1
                            except Exception as e:
                                logger.warning(f"Cleanup failed: {f.name}: {e}")

                    # Update scope tracking
                    for i in range(1, 4):
                        sd = config.paths.processed_dir / f"scope{i}"
                        last_pipeline_parquet[i] = set(f.name for f in sd.glob("*.parquet")) if sd.exists() else set()

                    _pipeline_status.update(
                        f"[Cycle {cycle_count}] Complete. {cleaned} scope files cleaned, {imported_events} total DB events."
                    )
                except Exception as e:
                    logger.exception("Monitor cycle failed")
                    _pipeline_status.update(f"[Cycle {cycle_count}] Failed: {e}")

                time.sleep(config.system.check_interval)

            _pipeline_status.finish({
                "mode": "monitor",
                "imported_total": imported_events,
                "stopped": "user_request",
            })

        except Exception as e:
            logger.exception("Monitor failed")
            _pipeline_status.fail(str(e))

    _monitor_thread = threading.Thread(target=_run, daemon=True, name="pipeline-monitor")
    _monitor_thread.start()
    return {"ok": True, "message": "Monitor started"}


def stop_monitor():
    """Request graceful stop of monitor loop."""
    global _stop_monitor
    _stop_monitor.set()
    return {"ok": True, "message": "Monitor stop requested"}


def stop_batch_pipeline():
    """Request graceful stop of batch pipeline."""
    global _stop_batch
    _stop_batch.set()
    return {"ok": True, "message": "Batch stop requested"}
