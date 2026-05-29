"""
Pipeline Manager — runs pipeline operations in background threads.

Manages batch pipeline, import, and monitor mode lifecycle
so the web server stays responsive.
"""

from __future__ import annotations

import json
import os
import queue
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.core.config import load_config, reset_config
from src.core.logger import get_logger

logger = get_logger(__name__)

KST = timezone(timedelta(hours=9))


def _ts(msg: str) -> str:
    """Prepend timestamp [HH:MM:SS] to a status message and print to stdout."""
    now = datetime.now(KST).strftime("%H:%M:%S")
    ts_msg = f"[{now}] {msg}"
    print(ts_msg, flush=True)
    return ts_msg


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

# ── Watchdog + Queue (watch event production) ──────────────────────
_watchdog_observers = []   # list of watchdog.Observer instances
_watchdog_lock = threading.Lock()
csv_event_queue: "queue.Queue" = queue.Queue()
# scope_tracker tracks new parquet files detected per scope (shared state)
# scope_tracker[scope_idx] = set of parquet filenames NEW since last pipeline
_scope_tracker: dict = {}      # set by csv_worker, read by monitor loop
_scope_tracker_lock = threading.Lock()


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

            _pipeline_status.update(_ts("Setting up directories..."))
            orch.setup_directories()

            _pipeline_status.update(_ts("Running preprocessor..."))
            if input_dirs:
                orch.run_preprocessor([Path(d) for d in input_dirs])
            else:
                orch.run_preprocessor()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update(_ts("Running grouper (merge)..."))
            grouper_result = orch.run_grouper()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update(_ts("Running classifier..."))
            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return
            orch.run_classifier()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update(_ts("Generating visualizations..."))
            orch.run_visualizer()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update(_ts("Generating reports..."))
            orch.run_reporter()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update(_ts("Sending emails..."))
            email_results = orch.run_email_sender()

            if _stop_batch.is_set():
                _pipeline_status.finish({"message": "Stopped by user"})
                return

            _pipeline_status.update(_ts("Importing to DB..."))
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

            _pipeline_status.update(_ts(f"Scanning {merged_dir}..."))
            if not merged_dir.exists():
                _pipeline_status.fail(_ts(f"Merged directory not found: {merged_dir}"))
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


def run_append(dirs: list, config_path: str = "config/config.yaml"):
    """Append: preprocess CSVs → merge → replace old parquets → import to DB.

    Accepts 3 scope directories. For each newly merged event:
    1. Delete existing event from DB (if same event_id)
    2. Replace parquet in data/merged/
    3. Import new event to DB
    """
    if _pipeline_status.running:
        return {"error": "Pipeline already running"}

    def _run():
        _pipeline_status.start("append")
        try:
            config = load_config(config_path)
            from src.pipeline.append_merge import run_append_merge
            import shutil
            from pathlib import Path

            project_root = Path(config_path).resolve().parent.parent
            merged_dir = (project_root / config.paths.merged_dir).resolve()
            merged_dir.mkdir(parents=True, exist_ok=True)

            # Temp output for append-merge
            temp_dir = merged_dir / ".append_work"
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

            _pipeline_status.update("Preprocessing and merging...")
            result = run_append_merge(
                input_dirs=[Path(d) for d in dirs],
                output_dir=temp_dir,
                keep_parquet=False,
            )

            grouper = result.get("grouper", {})
            merged_files = sorted(temp_dir.glob("event_*.parquet"))
            _pipeline_status.update(f"{len(merged_files)} merged events to process...")

            from src.db.schema import get_sync_connection, init_db_sync
            from src.db.repository import delete_event, get_event
            from src.import_job import run_import

            conn = get_sync_connection()
            replaced_count = 0
            imported_count = 0

            for fp in merged_files:
                event_id = fp.stem.replace("event_", "", 1)  # e.g. "20260509_073032"

                # 1. Delete old DB record + merged file
                existing = get_event(conn, event_id)
                if existing:
                    delete_event(conn, event_id)
                    replaced_count += 1

                # 2. Delete old parquet in merged/ if exists
                old_pq = merged_dir / fp.name
                if old_pq.exists():
                    old_pq.unlink()

                # 3. Copy new parquet
                shutil.copy2(fp, merged_dir / fp.name)

            conn.close()

            # 4. Classify, visualize, report (only for newly appended files)
            new_merged_paths = [merged_dir / fp.name for fp in merged_files]
            if new_merged_paths:
                try:
                    from src.orchestrator import SRFOrchestrator as _Orch
                    _orch = _Orch(config)
                    _pipeline_status.update("Classifying...")
                    _orch.run_classifier(merged_files=new_merged_paths)
                    _pipeline_status.update("Visualizing...")
                    _orch.run_visualizer(merged_files=new_merged_paths)
                    _pipeline_status.update("Generating reports...")
                    _orch.run_reporter(merged_files=new_merged_paths)
                except Exception as e:
                    logger.warning(f"Classification/visualization/report failed for appended events: {e}")

            # 5. Import to DB
            _pipeline_status.update("Importing to DB...")
            import_result = run_import(merged_dir, config_path)
            imported_count = import_result.get("imported", 0)

            # Cleanup temp
            if temp_dir.exists():
                shutil.rmtree(temp_dir)

            _pipeline_status.finish({
                "mode": "append",
                "matched_events": grouper.get("matched_events", 0),
                "total_files": grouper.get("total_files", 0),
                "replaced": replaced_count,
                "imported": imported_count,
            })

        except Exception as e:
            logger.exception("Append failed")
            _pipeline_status.fail(str(e))

    thread = threading.Thread(target=_run, daemon=True, name="pipeline-append")
    thread.start()
    return {"ok": True, "message": "Append started"}


def _wait_for_file_complete(fpath: Path, timeout: int = 60) -> int:
    """파일 크기가 안정화될 때까지 대기. 최종 크기 반환."""
    start = time.time()
    last_size = -1
    while True:
        try:
            with open(fpath, 'rb') as _f:
                _f.read(1)
            size = os.path.getsize(fpath)
        except (PermissionError, OSError):
            size = -1
        if size == last_size and size > 0:
            return size
        last_size = size
        if time.time() - start > timeout:
            return size if size > 0 else 0
        time.sleep(2.0)


def _is_valid_csv(fpath: Path) -> bool:
    return fpath.suffix.lower() == ".csv" and not fpath.name.endswith(":Zone.Identifier")


def _start_csv_worker(config, orch, processed_csv: set, stop_event: threading.Event):
    """
    Background thread: consume csv_event_queue, wait for file completion,
    run preprocessor, update scope_tracker.
    """
    global _scope_tracker

    scope_labels = {}
    for i in range(1, 4):
        wf = Path(config.paths.watch_folders[i - 1]) if i <= len(config.paths.watch_folders) else None
        scope_labels[i] = config.paths.processed_dir / f"scope{i}"

    while not stop_event.is_set():
        try:
            scope_idx, csv_path = csv_event_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        scope_dir = scope_labels.get(scope_idx)
        if scope_dir is None:
            csv_event_queue.task_done()
            continue

        scope_dir.mkdir(parents=True, exist_ok=True)
        pqt_path = scope_dir / f"{csv_path.stem}.parquet"

        # 중복 방지 (이미 처리됨)
        if csv_path.name in processed_csv or pqt_path.exists():
            if csv_path.name not in processed_csv:
                processed_csv.add(csv_path.name)
            csv_event_queue.task_done()
            continue

        # 파일 쓰기 완료 대기
        file_size = _wait_for_file_complete(csv_path)
        _pipeline_status.update(_ts(f"(scope{scope_idx}) Processing {csv_path.name}... ({int(file_size / 1024)}KB)"))

        try:
            success, reason, metadata = orch.preprocessor.process_one(csv_path, pqt_path, max_retries=3)
            if success and pqt_path.exists():
                processed_csv.add(csv_path.name)
                _pipeline_status.update(_ts(f"(scope{scope_idx}) ✓ {csv_path.name} → {pqt_path.name}"))
                # scope_tracker 업데이트
                with _scope_tracker_lock:
                    if scope_idx not in _scope_tracker:
                        _scope_tracker[scope_idx] = set()
                    _scope_tracker[scope_idx].add(pqt_path.name)
            else:
                logger.warning(f"(scope{scope_idx}) Failed {csv_path.name}: {reason}")
                processed_csv.add(csv_path.name)
                _pipeline_status.update(_ts(f"(scope{scope_idx}) ✗ {csv_path.name} FAILED: {reason}"))
        except Exception as e:
            logger.warning(f"(scope{scope_idx}) Failed {csv_path.name}: {e}")
            processed_csv.add(csv_path.name)
            _pipeline_status.update(_ts(f"(scope{scope_idx}) ✗ {csv_path.name} EXCEPTION: {e}"))

        csv_event_queue.task_done()


def _start_watchdogs(config, stop_event: threading.Event):
    """Watchdog Observer를 3개 폴더에 시작."""
    global _watchdog_observers

    from watchdog.observers import Observer
    from watchdog.events import FileSystemEventHandler

    class CSVHandler(FileSystemEventHandler):
        def __init__(self, scope_idx: int):
            self.scope_idx = scope_idx

        def on_created(self, event):
            if event.is_directory:
                return
            path = Path(event.src_path)
            if _is_valid_csv(path):
                csv_event_queue.put((self.scope_idx, path))

        def on_modified(self, event):
            if event.is_directory:
                return
            path = Path(event.src_path)
            if _is_valid_csv(path):
                csv_event_queue.put((self.scope_idx, path))

    with _watchdog_lock:
        _watchdog_observers = []
        for i in range(1, 4):
            wf_path = Path(config.paths.watch_folders[i - 1])
            if not wf_path.exists():
                wf_path.mkdir(parents=True, exist_ok=True)
                _pipeline_status.update(_ts(f"Created watch folder: {wf_path}"))
            handler = CSVHandler(scope_idx=i)
            obs = Observer()
            obs.schedule(handler, str(wf_path), recursive=False)
            obs.start()
            _watchdog_observers.append(obs)
            _pipeline_status.update(_ts(f"Watchdog started: {wf_path} (scope{i})"))


def _stop_watchdogs():
    global _watchdog_observers
    with _watchdog_lock:
        for obs in _watchdog_observers:
            obs.stop()
        for obs in _watchdog_observers:
            obs.join(timeout=3.0)
        _watchdog_observers = []


def start_monitor(config_path: str = "config/config.yaml"):
    """Start continuous folder monitoring in background."""
    global _monitor_thread, _stop_monitor, _scope_tracker

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

            for wf in config.paths.watch_folders:
                resolved = Path(wf)
                if not resolved.exists():
                    resolved.mkdir(parents=True, exist_ok=True)
                    _pipeline_status.update(_ts(f"Created watch folder: {resolved}"))

            # ── State ────────────────────────────────────────────────
            processed_csv = set()          # CSV files already processed
            last_pipeline_parquet = {}     # scope_idx → set of parquet names at last pipeline run
            processed_merged = set()       # merged file names already emailed + imported
            imported_events = 0
            cycle_count = 0
            wait_start: float | None = None
            _scope_tracker = {}            # reset scope_tracker

            merged_dir = config.paths.merged_dir

            # ── Initial snapshot ────────────────────────────────────
            existing_csv_count = 0
            for i, wf in enumerate(config.paths.watch_folders, 1):
                wf_path = Path(wf)
                if wf_path.exists():
                    for f in wf_path.glob("*.csv"):
                        if _is_valid_csv(f):
                            processed_csv.add(f.name)
                            existing_csv_count += 1

            for i in range(1, 4):
                sd = config.paths.processed_dir / f"scope{i}"
                last_pipeline_parquet[i] = set(
                    f.name for f in sd.glob("*.parquet")
                ) if sd.exists() else set()

            processed_merged.update(f.name for f in merged_dir.glob("*.parquet"))

            _pipeline_status.update(
                f"Initial: {existing_csv_count} existing CSV ignored, "
                f"{sum(len(v) for v in last_pipeline_parquet.values())} scope parquet, "
                f"{len(processed_merged)} merged tracked. "
                f"Waiting for NEW CSV files (watchdog)..."
            )

            # ── Start watchdog + csv_worker ─────────────────────────
            _start_watchdogs(config, _stop_monitor)
            csv_worker_thread = threading.Thread(
                target=_start_csv_worker,
                args=(config, orch, processed_csv, _stop_monitor),
                daemon=True,
                name="csv-worker",
            )
            csv_worker_thread.start()

            # ── Main monitor loop (pipeline decision only) ──────────
            while not _stop_monitor.is_set():
                wait_timeout = config.grouper.wait_timeout
                run_pipeline = False

                # Snapshot scope_tracker
                with _scope_tracker_lock:
                    current_new = dict(_scope_tracker)  # copy

                all_ready = len(current_new) >= 3
                ready_scopes = sorted(current_new.keys())
                missing_scopes = [i for i in range(1, 4) if i not in current_new]

                if current_new and not all_ready:
                    # 파일 도착 중 — 대기 모드
                    if wait_start is None:
                        wait_start = time.time()

                    elapsed = time.time() - wait_start
                    remaining = max(0, wait_timeout - int(elapsed))
                    _pipeline_status.update(_ts(
                        f"Waiting for all 3 scopes... "
                        f"have: {ready_scopes}, "
                        f"missing: {missing_scopes}, "
                        f"timeout in {remaining}s"
                    ))

                    if elapsed >= wait_timeout:
                        wait_start = None
                        _pipeline_status.update(_ts(
                            f"Timeout ({wait_timeout}s) — running pipeline with partial data..."
                        ))
                        run_pipeline = True
                    else:
                        time.sleep(config.system.check_interval)
                        continue

                elif not current_new and wait_start is not None:
                    elapsed = time.time() - wait_start
                    remaining = max(0, wait_timeout - int(elapsed))
                    _pipeline_status.update(_ts(
                        f"Waiting for all 3 scopes... "
                        f"have: {ready_scopes}, "
                        f"missing: {missing_scopes}, "
                        f"timeout in {remaining}s"
                    ))
                    if elapsed >= wait_timeout:
                        wait_start = None
                        _pipeline_status.update(_ts(
                            f"Timeout ({wait_timeout}s) — running pipeline with partial data..."
                        ))
                        run_pipeline = True
                    else:
                        time.sleep(config.system.check_interval)
                        continue

                elif all_ready and wait_start is not None:
                    _pipeline_status.update(_ts("All 3 scopes ready — starting pipeline"))
                    wait_start = None
                    run_pipeline = True

                elif not current_new and wait_start is None:
                    time.sleep(config.system.check_interval)
                    continue

                if not current_new and not run_pipeline:
                    time.sleep(config.system.check_interval)
                    continue

                if not all_ready and not run_pipeline:
                    time.sleep(config.system.check_interval)
                    continue

                # ── Capture new scope files ─────────────────────────
                new_scope_files = {}
                for i in range(1, 4):
                    sd = config.paths.processed_dir / f"scope{i}"
                    current = set(f for f in sd.glob("*.parquet")) if sd.exists() else set()
                    old = last_pipeline_parquet.get(i, set())
                    new_scope_files[i] = [f for f in current if f.name not in old]

                # ── Pipeline execution ──────────────────────────────
                cycle_count += 1
                _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Running pipeline..."))
                try:
                    for step_name, step_fn in [
                        ("Grouper (merge)", orch.run_grouper),
                        ("Classifier", orch.run_classifier),
                        ("Visualizer", orch.run_visualizer),
                        ("Reporter", orch.run_reporter),
                    ]:
                        _pipeline_status.update(_ts(f"[Cycle {cycle_count}] {step_name}..."))
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
                            _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Emails sent: {emailed}"))
                            processed_merged.update(f.name for f in new_merged)
                        else:
                            _pipeline_status.update(_ts(f"[Cycle {cycle_count}] No new merged files"))
                    else:
                        _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Email sender disabled"))

                    _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Importing to DB..."))
                    db_count = orch.import_to_db()
                    imported_events += db_count

                    # Cleanup: delete NEW scope parquet files
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

                    # Reset scope_tracker
                    with _scope_tracker_lock:
                        _scope_tracker = {}

                    _pipeline_status.update(_ts(
                        f"[Cycle {cycle_count}] Complete. {cleaned} scope files cleaned, {imported_events} total DB events."
                    ))
                except Exception as e:
                    logger.exception("Monitor cycle failed")
                    _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Failed: {e}"))

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
    """Request graceful stop of monitor loop and watchdogs."""
    global _stop_monitor
    _stop_monitor.set()
    _stop_watchdogs()
    return {"ok": True, "message": "Monitor stop requested"}


def stop_batch_pipeline():
    """Request graceful stop of batch pipeline."""
    global _stop_batch
    _stop_batch.set()
    return {"ok": True, "message": "Batch stop requested"}
