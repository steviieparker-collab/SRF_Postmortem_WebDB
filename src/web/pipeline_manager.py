"""
Pipeline Manager — runs pipeline operations in background threads.

Manages batch pipeline, import, and monitor mode lifecycle
so the web server stays responsive.

Monitor mode (start_monitor) uses main_v0.4-style approach:
- Each CSV is preprocessed immediately on detection (no waiting for all 3 scopes)
- After preprocessing, tracks which scope parquets are ready
- When all 3 scope parquets are ready OR timeout expires -> run merge+classify+visualize+import
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


def _drain_queue(q):
    """Remove all remaining items from queue."""
    while True:
        try:
            q.get_nowait()
            q.task_done()
        except queue.Empty:
            break


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

# ── Monitor state ────────────────────────────────────
# scope_parquet_tracker[scope_idx] = {csv_filename: parquet_filename} ready since last pipeline
_scope_parquet_tracker: dict[int, dict[str, str]] = {}
_scope_parquet_lock = threading.Lock()
_monitor_start_time: float = 0     # when the first CSV arrived for current cycle
_monitor_scope_timeout: int = 120  # seconds


def _get_scope_csv_path(config, scope_idx: int) -> Path:
    """Get the watch folder path for a given scope index (1-indexed)."""
    return Path(config.paths.watch_folders[scope_idx - 1])


def _get_scope_parquet_dir(config, scope_idx: int) -> Path:
    """Get the processed/parquet output directory for a scope."""
    return config.paths.processed_dir / f"scope{scope_idx}"


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
    """Append: preprocess CSVs -> merge -> replace old parquets -> import to DB.

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

            # 4. Run classifier ONLY on NEW merged files to update sequence_info.json
            _pipeline_status.update("Running classifier on new merged files only...")
            try:
                from src.pipeline.classifier import AcceleratorEventClassifier
                clf = AcceleratorEventClassifier()
                # Only pass new files — NOT the entire merged_dir
                new_files = [str(merged_dir / fp.name) for fp in merged_files]
                if new_files:
                    clf.run(str(merged_dir), str(config.paths.results_dir), input_files=new_files)
                    _pipeline_status.update(f"Classifier done: {len(new_files)} files.")
                else:
                    _pipeline_status.update("No new files to classify.")
            except Exception as clf_err:
                _pipeline_status.update(f"Classifier step warning: {clf_err}")

            # 5. Import to DB — use pre-computed classifications from sequence_info.json
            _pipeline_status.update("Importing to DB...")
            from src.import_job import _load_sequence_info_classifications
            classifications = _load_sequence_info_classifications(merged_dir, config.paths.results_dir)
            if classifications:
                from src.import_job import run_import_with_classifications
                import_result = run_import_with_classifications(merged_dir, classifications, config_path)
            else:
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


# ── File helpers ──────────────────────────────────────

def _wait_for_file_complete(fpath: Path, timeout: int = 60) -> int:
    """
    Wait until file write completes.
    Strategy:
    1. Wait MIN_WAIT seconds first (CSV takes ~60s to write)
    2. Then check if size stabilizes (2 retries, 2s apart)
    Returns final file size, or 0 on timeout/error.
    """
    import time as _time
    MIN_WAIT = 60  # minimum wait before checking stability
    _time.sleep(MIN_WAIT)

    start = _time.time()
    last_size = -1
    stable_count = 0
    while True:
        try:
            with open(fpath, 'rb') as _f:
                _f.read(1)
            size = os.path.getsize(fpath)
        except (PermissionError, OSError):
            size = -1
        if size == last_size and size > 0:
            stable_count += 1
            if stable_count >= 2:  # 2 consecutive same size = stable
                return size
        else:
            stable_count = 0
        last_size = size
        if _time.time() - start + MIN_WAIT > timeout:
            return size if size > 0 else 0
        _time.sleep(2.0)


def _is_valid_csv(fpath: Path) -> bool:
    return fpath.suffix.lower() == ".csv" and not fpath.name.endswith(":Zone.Identifier")


# ── CSV Detector: detect & wait for file, NO preprocessing ──

def _start_csv_worker(config, orch, processed_csv: set, stop_event: threading.Event):
    """
    ONLY detect CSV, wait for write complete, and register in scope_tracker.
    Do NOT preprocess here — preprocessing happens in the pipeline cycle.
    This way, all 3 scope CSVs are detected quickly without blocking on preprocess.
    """
    global _scope_parquet_tracker, _monitor_start_time

    while not stop_event.is_set():
        try:
            scope_idx, csv_path = csv_event_queue.get(timeout=1.0)
        except queue.Empty:
            continue

        # Deduplication
        if csv_path.name in processed_csv:
            csv_event_queue.task_done()
            continue

        # Wait for file write to complete (up to 120s for big CSVs)
        file_size = _wait_for_file_complete(csv_path, timeout=120)
        processed_csv.add(csv_path.name)

        if file_size <= 0:
            logger.warning(f"(scope{scope_idx}) {csv_path.name}: not ready after 120s, skipping")
            csv_event_queue.task_done()
            continue

        # ── Just register in tracker (NO preprocess here) ────────
        _pipeline_status.update(_ts(f"(scope{scope_idx}) ✓ {csv_path.name} ready ({int(file_size/1024)}KB)"))

        with _scope_parquet_lock:
            if scope_idx not in _scope_parquet_tracker:
                _scope_parquet_tracker[scope_idx] = {}
            _scope_parquet_tracker[scope_idx][csv_path.name] = csv_path.name  # store csv name, not parquet yet

            # Start timeout timer on first CSV arrival
            if _monitor_start_time == 0:
                _monitor_start_time = time.time()

        scope_label = {1: 'W:', 2: 'X:', 3: 'Y:'}.get(scope_idx, f'Scope{scope_idx}')
        _pipeline_status.update(_ts(
            f"{scope_label} ready, scopes: {sorted(_scope_parquet_tracker.keys())}"
        ))

        csv_event_queue.task_done()


# ── Watchdog setup ────────────────────────────────────

def _start_watchdogs(config, stop_event: threading.Event):
    """Start watchdog observers on all 3 watch folders.
    Network drives (W:\) may not trigger watchdog reliably,
    so this is kept as best-effort supplement — the poller does the real work.
    """
    global _watchdog_observers

    try:
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



        with _watchdog_lock:
            _watchdog_observers = []
            for i in range(1, 4):
                wf_path = Path(config.paths.watch_folders[i - 1])
                if not wf_path.exists():
                    _pipeline_status.update(_ts(f"Watch folder not found: {wf_path} (scope{i})"))
                    continue
                handler = CSVHandler(scope_idx=i)
                obs = Observer()
                obs.schedule(handler, str(wf_path), recursive=False)
                obs.start()
                _watchdog_observers.append(obs)
                _pipeline_status.update(_ts(f"Watchdog started (best-effort): {wf_path} (scope{i})"))
    except Exception as e:
        _pipeline_status.update(_ts(f"Watchdog init failed (ok, poller will handle): {e}"))


def _stop_watchdogs():
    global _watchdog_observers
    with _watchdog_lock:
        for obs in _watchdog_observers:
            obs.stop()
        for obs in _watchdog_observers:
            obs.join(timeout=3.0)
        _watchdog_observers = []


# ── Scope readiness timer thread ─────────────────────

def _scope_readiness_checker(config, orch, stop_event: threading.Event):
    """
    Periodic check: when all 3 scopes are ready OR timeout expires,
    run the pipeline (grouper -> classifier -> visualizer -> import).
    """
    global _scope_parquet_tracker, _monitor_start_time, _monitor_scope_timeout

    # Track state across pipeline cycles
    last_pipeline_parquet: dict[int, set] = {i: set() for i in range(1, 4)}
    processed_merged: set = set()
    imported_events = 0
    cycle_count = 0

    # Take initial snapshot of existing scope parquets
    for i in range(1, 4):
        sd = _get_scope_parquet_dir(orch.config, i)
        if sd.exists():
            last_pipeline_parquet[i] = set(f.name for f in sd.glob("*.parquet"))

    # Take initial snapshot of existing merged files
    merged_dir = orch.config.paths.merged_dir
    processed_merged.update(f.name for f in merged_dir.glob("*.parquet"))

    while not stop_event.is_set():
        time.sleep(1.0)

        with _scope_parquet_lock:
            current_tracker = dict(_scope_parquet_tracker)  # shallow copy
            start_time = _monitor_start_time

        # Nothing tracked yet — nothing to do
        if not current_tracker:
            continue

        ready_scopes = sorted(current_tracker.keys())
        missing_scopes = [i for i in range(1, 4) if i not in current_tracker]
        all_ready = len(ready_scopes) >= 3

        # Check timeout
        elapsed = time.time() - start_time if start_time > 0 else 0
        timeout_reached = elapsed >= _monitor_scope_timeout

        if not timeout_reached and not all_ready:
            # Still waiting — update status
            remaining = max(0, int(_monitor_scope_timeout - elapsed)) if start_time > 0 else _monitor_scope_timeout
            scope_labels = {s: {1:'W:',2:'X:',3:'Y:'}.get(s,f'Scope{s}') for s in ready_scopes}
            missing_labels = {m: {1:'W:',2:'X:',3:'Y:'}.get(m,f'Scope{m}') for m in missing_scopes}
            _pipeline_status.update(_ts(
                f"Waiting... ready: {list(scope_labels.values())}, "
                f"missing: {list(missing_labels.values())}, timeout in {remaining}s"
            ))
            continue

        # ── Pipeline trigger condition met ────────
        if all_ready:
            _pipeline_status.update(_ts("All 3 scopes ready — running pipeline"))
        elif timeout_reached:
            _pipeline_status.update(_ts(
                f"Timeout ({_monitor_scope_timeout}s) — running pipeline with "
                f"scopes {ready_scopes}"
            ))

        cycle_count += 1

        # ── Consume tracker (reset for next cycle) ──
        with _scope_parquet_lock:
            tracker_snapshot = dict(_scope_parquet_tracker)
            _scope_parquet_tracker = {}
            _monitor_start_time = 0

        # ── Step 1: Preprocess all detected CSVs → parquet ──
        csv_names: list[str] = []
        for scope_idx, csv_map in tracker_snapshot.items():
            for csv_name in csv_map.values():
                csv_names.append(csv_name)

        if not csv_names:
            _pipeline_status.update(_ts("No CSV files to process — skipping pipeline"))
            continue

        _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Preprocessing {len(csv_names)} CSVs..."))

        preprocessed_scopes: set[int] = set()
        for scope_idx, csv_map in tracker_snapshot.items():
            scope_dir = _get_scope_parquet_dir(orch.config, scope_idx)
            scope_dir.mkdir(parents=True, exist_ok=True)
            for csv_name in csv_map.values():
                csv_path = _get_scope_csv_path(orch.config, scope_idx) / csv_name
                if not csv_path.exists():
                    logger.warning(f"CSV disappeared: {csv_path}")
                    continue
                pqt_path = scope_dir / f"{Path(csv_name).stem}.parquet"
                if pqt_path.exists():
                    _pipeline_status.update(_ts(f"  (scope{scope_idx}) {csv_name} → parquet exists, skip"))
                else:
                    _pipeline_status.update(_ts(f"  (scope{scope_idx}) Processing {csv_name}..."))
                    try:
                        success, reason, _ = orch.preprocessor.process_one(csv_path, pqt_path, max_retries=3)
                        if success:
                            _pipeline_status.update(_ts(f"  (scope{scope_idx}) ✓ {csv_name} → {pqt_path.name}"))
                        else:
                            _pipeline_status.update(_ts(f"  (scope{scope_idx}) ✗ {csv_name} failed: {reason}"))
                    except Exception as e:
                        _pipeline_status.update(_ts(f"  (scope{scope_idx}) ✗ {csv_name} error: {e}"))
                preprocessed_scopes.add(scope_idx)

        # ── Step 2: Run pipeline (grouper/classifier/visualizer/import) ──
        _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Running pipeline from scopes {sorted(preprocessed_scopes)}..."))

        try:
            enough_for_merge = len(preprocessed_scopes) >= 2
            if enough_for_merge:
                _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Grouper (merge)..."))
                orch.run_grouper()
            else:
                _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Only {len(preprocessed_scopes)} scope — skipping merge"))

            # NEW merged files since last cycle
            new_merged = [f for f in merged_dir.glob("*.parquet") if f.name not in processed_merged]

            if new_merged:
                _pipeline_status.update(_ts(f"[Cycle {cycle_count}] {len(new_merged)} new merged, running classifier/visualizer/reporter..."))
                orch.run_classifier()
                orch.run_visualizer()
                orch.run_reporter()
                processed_merged.update(f.name for f in new_merged)

            # Import to DB (import_job checks duplicates)
            _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Importing to DB..."))
            db_count = orch.import_to_db()
            imported_events += db_count

            # Cleanup: remove NEW scope parquet files
            cleaned = 0
            for i in range(1, 4):
                sd = _get_scope_parquet_dir(orch.config, i)
                if sd.exists():
                    for f in sorted(sd.glob("*.parquet")):
                        if f.name not in last_pipeline_parquet.get(i, set()):
                            try:
                                f.unlink()
                                cleaned += 1
                            except Exception as e:
                                logger.warning(f"Cleanup {f.name}: {e}")

            # Update tracking
            for i in range(1, 4):
                sd = _get_scope_parquet_dir(orch.config, i)
                last_pipeline_parquet[i] = set(f.name for f in sd.glob("*.parquet")) if sd.exists() else set()

            _pipeline_status.update(_ts(
                f"[Cycle {cycle_count}] Complete. {cleaned} files cleaned, "
                f"{imported_events} total DB events."
            ))

        except Exception as e:
            logger.exception("Monitor cycle failed")
            _pipeline_status.update(_ts(f"[Cycle {cycle_count}] Failed: {e}"))


# ── start_monitor / stop_monitor ──────────────────────

def start_monitor(config_path: str = "config/config.yaml"):
    """
    Full pipeline: watchdog detect CSV -> graphs -> parquet -> grouper -> classifier -> visualizer -> reporter -> email -> DB.
    This is the main entry point used by "Full Monitor" button.
    """
    return start_watchdog_only(config_path)


def start_watchdog_only(config_path: str = "config/config.yaml"):
    """
    Full pipeline: watchdog detect CSV &#8594; graphs &#8594; parquet &#8594; grouper &#8594; classifier &#8594; visualizer &#8594; reporter &#8594; email &#8594; DB.
    """
    global _watchdog_observers, _scope_parquet_tracker, _monitor_start_time, _stop_monitor

    if _pipeline_status.running:
        return {"error": "Pipeline already running"}

    # Reset stop event in case of previous stop
    _stop_monitor = threading.Event()
    _pipeline_status.start("monitor")

    try:
        config = load_config(config_path)
        from src.orchestrator import SRFOrchestrator
        orch = SRFOrchestrator(config)

        _start_watchdogs(config, threading.Event())
    except Exception as e:
        _pipeline_status.fail(str(e))
        return {"error": str(e)}

    _reset_pending_called = False
    _pending_scopes: dict[int, Path] = {}
    _first_csv_time: float = 0
    _all_csvs_arrived: bool = False
    _narrow_graph_paths: dict[int, str] = {}  # scope_idx -> narrow graph file path

    def _reset_pending():
        nonlocal _pending_scopes, _first_csv_time, _all_csvs_arrived, _narrow_graph_paths
        _pending_scopes.clear()
        _narrow_graph_paths.clear()
        _first_csv_time = 0
        _all_csvs_arrived = False

    def _full_worker():
        """
        v2: Queue consumer that collects exactly 3 CSVs, then processes full pipeline.
        - No dedup (_processed_watchdog removed)
        - 3/3 collected -> 60s wait -> beam check (all 3, <3V = fail+IDLE)
        - Individual scope processing (graph + parquet), fail stops all
        - 3 parquet check -> grouper -> classifier -> visualizer -> reporter -> email -> DB -> IDLE
        """
        nonlocal _pending_scopes, _first_csv_time, _all_csvs_arrived
        output_dir = Path(config_path).parent.parent / "data" / "graph_only"
        output_dir.mkdir(parents=True, exist_ok=True)
        merged_dir = config.paths.merged_dir
        merged_dir.mkdir(parents=True, exist_ok=True)
        processed_dir = config.paths.processed_dir

        # Clean processed scope parquets from previous run
        for si in range(1, 4):
            sd = processed_dir / f"scope{si}"
            if sd.exists():
                for f in sd.glob("*.parquet"):
                    try: f.unlink()
                    except: pass

        while not _stop_monitor.is_set():
            try:
                scope_idx, csv_path = csv_event_queue.get(timeout=1.0)
            except queue.Empty:
                if _stop_monitor.is_set():
                    break
                # Timeout check: 120s since first CSV with no 3/3
                if _first_csv_time > 0 and not _all_csvs_arrived:
                    elapsed = time.time() - _first_csv_time
                    if elapsed >= 120:
                        _pipeline_status.update(_ts(f"[FAIL] 120s timeout: only {len(_pending_scopes)} CSVs arrived. Returning to IDLE."))
                        _reset_pending()
                        _ts("Drained stale CSV events from queue")
                        _drain_queue(csv_event_queue)
                continue

            if _stop_monitor.is_set():
                break

            # Collect CSV (no dedup, _processed_watchdog removed)
            _pending_scopes[scope_idx] = csv_path
            _ts(f"CSV arrived: {Path(csv_path).parent.name}/{csv_path.name} (scope{scope_idx}) — {len(_pending_scopes)}/3 collected")

            # Start timeout on first CSV
            if _first_csv_time == 0:
                _first_csv_time = time.time()

            # 3/3 not yet — continue waiting
            if len(_pending_scopes) < 3:
                continue

            if _stop_monitor.is_set():
                break

            # ── 3/3 collected ────
            _all_csvs_arrived = True
            _ts(f"All 3 CSVs collected. Waiting 60s for file writes to complete...")
            time.sleep(60)

            if _stop_monitor.is_set():
                break

            # ── Beam check: all 3 scopes at once ──
            _ts("Checking beam current for all 3 scopes...")
            beam_fail = False
            for sidx in sorted(_pending_scopes.keys()):
                csv_path = _pending_scopes[sidx]
                try:
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        lines_for_beam = f.readlines()
                    beam_val_str = lines_for_beam[21].split(',')[1].strip()
                    beam_val = float(beam_val_str)
                    if beam_val < 3:
                        _pipeline_status.update(_ts(f"[FAIL] Low beam: (scope{sidx}) beam current {beam_val}V < 3V. Returning to IDLE."))
                        beam_fail = True
                    else:
                        _ts(f"  (scope{sidx}) beam current {beam_val}V ✓")
                except Exception as e:
                    _pipeline_status.update(_ts(f"[FAIL] (scope{sidx}) beam check error: {e}. Returning to IDLE."))
                    beam_fail = True

            if beam_fail:
                _reset_pending()
                _ts("Drained stale CSV events from queue")
                _drain_queue(csv_event_queue)
                continue

            # ── Process individual scopes: graph + parquet ──
            all_parquet_ok = True
            for sidx in sorted(_pending_scopes.keys()):
                scope_idx = sidx
                csv_path = _pending_scopes[sidx]

                if _stop_monitor.is_set():
                    break

                # Quick read check
                try:
                    with open(csv_path, 'rb') as f:
                        f.read(1)
                    file_size = os.path.getsize(csv_path)
                except:
                    _pipeline_status.update(_ts(f"[FAIL] preprocessor error: (scope{scope_idx}) Cannot read: {csv_path.name}. Returning to IDLE."))
                    all_parquet_ok = False
                    break
                if file_size <= 0:
                    _pipeline_status.update(_ts(f"[FAIL] preprocessor error: (scope{scope_idx}) Empty: {csv_path.name}. Returning to IDLE."))
                    all_parquet_ok = False
                    break

                _ts(f"(scope{scope_idx}) Processing {csv_path.name} ({int(file_size/1024)}KB)...")

                try:
                    import io
                    import pandas as pd
                    from matplotlib import pyplot as plt
                    import numpy as np

                    scope_label = {1: 'W:', 2: 'X:', 3: 'Y:'}.get(scope_idx, f'scope{scope_idx}')
                    folder_out = output_dir / scope_label.replace(':', '')
                    folder_out.mkdir(parents=True, exist_ok=True)

                    with open(csv_path, 'r', encoding='utf-8') as f:
                        lines = f.readlines()
                    labels = lines[19].strip().split(',')
                    df = pd.read_csv(io.StringIO(''.join(lines[21:])), header=None)
                    if len(labels) > 6:
                        labels.pop(6)
                    if df.shape[1] > 6:
                        df.drop(columns=[6], inplace=True)
                    df.columns = labels
                    time_col = labels[0]

                    stem = csv_path.stem
                    BG = '#0f1117'
                    PANEL = '#1a1d27'
                    GRID = '#2a2d3a'
                    TXT = '#e0e0e0'
                    TITLE = '#ffffff'
                    ACOL = {'B': '#FFE033', 'C': '#00D4FF', 'D': '#FF1E6B', 'E': '#96D800'}
                    DPAL = ['#7986CB', '#4DB6AC', '#FFB74D', '#E57373', '#BA68C8', '#4DD0E1', '#AED581', '#F06292',
                            '#64B5F6', '#A1887F', '#90A4AE', '#FFF176', '#80CBC4', '#CE93D8', '#FFCC02', '#80DEEA']

                    def make_plot(tmin, tmax, suffix):
                        mask = (df[time_col] >= tmin) & (df[time_col] <= tmax)
                        dd = df[mask].reset_index(drop=True)
                        sp = folder_out / f"{stem}_{suffix}.jpg"
                        plt.rcParams.update({'font.family': 'DejaVu Sans', 'font.size': 10,
                            'axes.facecolor': PANEL, 'figure.facecolor': BG, 'axes.edgecolor': GRID,
                            'axes.labelcolor': TXT, 'xtick.color': TXT, 'ytick.color': TXT, 'text.color': TXT,
                            'grid.color': GRID, 'grid.linestyle': '--', 'grid.linewidth': 0.6, 'grid.alpha': 0.8})
                        fig, ax = plt.subplots(figsize=(15, 8))
                        fig.patch.set_facecolor(BG)
                        bcol, ccol, dcol, ecol = labels[1], labels[2], labels[3], labels[4]
                        dig_cols = dd.columns[6:22]
                        ana_cols = [c for c in dd.columns if c not in dig_cols and c != time_col]
                        tv = dd[time_col].values * 1000
                        xl = tmin * 1000
                        cmap = {bcol: ('B', lambda v: v / 2.0 * 7), ccol: ('C', lambda v: v * 35),
                                dcol: ('D', lambda v: v * 35), ecol: ('E', lambda v: v * 35)}
                        for c in ana_cols:
                            if c not in cmap:
                                continue
                            k, fn = cmap[c]
                            cl = ACOL[k]
                            sv = fn(dd[c].values)
                            ax.plot(tv, sv, color=cl, lw=2.2, alpha=0.85, label=c)
                            ax.plot(tv, sv, color=cl, lw=0.7, alpha=1.0, label='_nolegend_')
                            iy = float(fn(df[c].iloc[0]))
                            ax.plot(xl, iy, marker='<', ms=11, mfc=cl, mec='white', mew=1.2, zorder=6, clip_on=False)
                            ax.annotate(f'{df[c].iloc[0]:.3f}', (xl, iy), xytext=(6, 0), textcoords='offset points', fontsize=7.5, color=cl, alpha=0.85, va='center')
                        gap, sy = 1.2, 38
                        for i, c in enumerate(dig_cols):
                            yv = dd[c].fillna(0).values
                            yb = sy - i * gap
                            dc = DPAL[i % len(DPAL)]
                            ax.axhspan(yb - 0.05, yb + 1.1, alpha=0.04, color=dc, zorder=0)
                            ax.step(tv, yv + yb, where='post', color=dc, lw=1.1, alpha=0.9)
                            ax.text(tv[0], yb + 0.25, c, fontsize=8, color=dc, ha='left', va='bottom',
                                bbox=dict(boxstyle='round,pad=0.15', fc=PANEL, ec=dc, alpha=0.7, lw=0.6))
                        ax.set_xlabel("Time (ms)", color=TXT)
                        ax.set_ylabel("Amplitude (a.u.)", color=TXT)
                        ax.set_title(f"SRF Postmortem | {scope_label} | {tmin * 1000:.1f}~{tmax * 1000:+.1f}ms", fontsize=12, color=TITLE, fontweight='bold')
                        ax.set_ylim(dd[ana_cols].min().min() - 1 if not dd[ana_cols].empty else -1, 41)
                        ax.set_xlim(tv[0], tv[-1])
                        ax.grid(True)
                        for s in ax.spines.values():
                            s.set_edgecolor(GRID)
                        leg = ax.legend(loc='upper left', bbox_to_anchor=(1.02, 1),
                            frameon=True, handlelength=2.0, handleheight=1.2, labelspacing=0.6)
                        leg.get_frame().set_facecolor(PANEL)
                        leg.get_frame().set_edgecolor(GRID)
                        leg.get_frame().set_alpha(0.9)
                        for t in leg.get_texts():
                            t.set_color('white')
                        fig.subplots_adjust(left=0.05, right=0.78, top=0.92, bottom=0.08)
                        plt.savefig(str(sp), dpi=150, bbox_inches='tight', facecolor=BG)
                        plt.close()
                        plt.rcParams.update(plt.rcParamsDefault)
                        return sp

                    # Track narrow graph path for email attachment (3 scope images)
                    _narrow_graph_paths[sidx] = str(make_plot(-0.001, 0.001, "narrow"))
                    make_plot(-0.05, 0.05, "wide")
                    _ts(f"(scope{scope_idx}) ✓ {csv_path.name} -> graphs saved")

                    # ── Convert to parquet ──
                    scope_pq_dir = processed_dir / f"scope{scope_idx}"
                    scope_pq_dir.mkdir(parents=True, exist_ok=True)
                    pqt_path = scope_pq_dir / f"{csv_path.stem}.parquet"
                    if not pqt_path.exists():
                        try:
                            success, reason, _ = orch.preprocessor.process_one(csv_path, pqt_path, max_retries=2)
                            if success:
                                _ts(f"(scope{scope_idx}) ✓ {csv_path.name} -> parquet")
                            else:
                                _pipeline_status.update(_ts(f"[FAIL] preprocessor error: (scope{scope_idx}) ✗ parquet fail: {reason}. Returning to IDLE."))
                                all_parquet_ok = False
                                break
                        except Exception as e:
                            _pipeline_status.update(_ts(f"[FAIL] preprocessor error: (scope{scope_idx}) ✗ parquet error: {e}. Returning to IDLE."))
                            all_parquet_ok = False
                            break

                    if _stop_monitor.is_set():
                        break

                    # 2s sleep between scopes
                    time.sleep(2.0)

                except Exception as e:
                    _pipeline_status.update(_ts(f"[FAIL] preprocessor error: (scope{scope_idx}) ✗ {csv_path.name} error: {e}. Returning to IDLE."))
                    all_parquet_ok = False
                    break

            if _stop_monitor.is_set():
                break

            if not all_parquet_ok:
                _reset_pending()
                _ts("Drained stale CSV events from queue")
                _drain_queue(csv_event_queue)
                continue

            # ── Verify 3 parquets exist ──
            scopes_with_parquet = []
            for si in range(1, 4):
                sd = processed_dir / f"scope{si}"
                if sd.exists() and list(sd.glob("*.parquet")):
                    scopes_with_parquet.append(si)

            if len(scopes_with_parquet) < 3:
                _pipeline_status.update(_ts(f"[FAIL] parquet incomplete: Only {len(scopes_with_parquet)} scopes have parquets (need 3). Returning to IDLE."))
                _reset_pending()
                _ts("Drained stale CSV events from queue")
                _drain_queue(csv_event_queue)
                continue

            # ── ALL 3 parquets ready — run pipeline ──
            _pipeline_status.update(_ts("All 3 scopes ready. Running grouper..."))
            try:
                orch.run_grouper()

                temp_dir = merged_dir / "temp"
                temp_files = sorted(temp_dir.glob("*.parquet"))

                if temp_files:
                    _pipeline_status.update(_ts(f"Grouper: {len(temp_files)} new files in temp/. Moving to merged/..."))
                    new_paths = []
                    for f in temp_files:
                        dest = merged_dir / f.name
                        import shutil
                        shutil.move(str(f), str(dest))
                        new_paths.append(str(dest))

                    _pipeline_status.update(_ts(f"Running classifier/visualizer/reporter on {len(new_paths)} new files..."))
                    orch.run_classifier(merged_files=new_paths)
                    _pipeline_status.update(_ts("Classifier done."))
                    orch.run_visualizer(merged_files=new_paths)
                    _pipeline_status.update(_ts("Visualizer done."))
                    orch.run_reporter(merged_files=new_paths)
                    _pipeline_status.update(_ts("Reporter done."))
                else:
                    _pipeline_status.update(_ts("No new merged files from grouper. Skipping classifier/visualizer/reporter."))

                _pipeline_status.update(_ts("Sending email reports with graphs + WebDB link..."))
                emailed = 0
                if orch.email_sender:
                    url_base = getattr(config.web, 'url_base', '')
                    for mf in [Path(p) for p in new_paths]:
                        cls_file = orch.config.paths.results_dir / f"{mf.stem}_classification.json"
                        report_file = orch.config.paths.reports_dir / f"{mf.stem}_report.md"
                        event_id = mf.stem.replace('event_', '')
                        summary = "Unclassified"
                        if cls_file.exists():
                            try:
                                summary = json.loads(cls_file.read_text(encoding='utf-8')).get('description', summary)
                            except:
                                pass
                        try:
                            content = report_file.read_text(encoding='utf-8') if report_file.exists() else ""
                            # Build event_url: public (Cloudflare) only
                            event_url = f"{url_base.rstrip('/')}/events/{event_id}" if url_base else None
                            # Attach 3 scope narrow graphs separately
                            graph_files = list(_narrow_graph_paths.values())
                            orch.email_sender.send_report(
                                report_content=content, report_format='markdown',
                                graph_files=graph_files if graph_files else None,
                                classification_summary=summary,
                                event_url=event_url,
                            )
                            emailed += 1
                        except Exception as e:
                            _ts(f"Email failed for {mf.name}: {e}")
                    _pipeline_status.update(_ts(f"Emails sent: {emailed}"))

                _pipeline_status.update(_ts("Importing to DB..."))
                db_count = orch.import_to_db()
                _pipeline_status.update(_ts(f"Pipeline complete. DB imported: {db_count}"))

                # Cleanup processed scope parquets
                for si in range(1, 4):
                    sd = processed_dir / f"scope{si}"
                    if sd.exists():
                        for f in sd.glob("*.parquet"):
                            try:
                                f.unlink()
                            except:
                                pass
            except Exception as e:
                _pipeline_status.update(_ts(f"[FAIL] pipeline error: {e}"))

            # task_done + reset pending — return to IDLE
            for _ in range(len(_pending_scopes)):
                csv_event_queue.task_done()
            _reset_pending()
            _ts("Drained stale CSV events from queue")
            _drain_queue(csv_event_queue)
            _pipeline_status.update("Monitoring for next batch of CSVs...")

        # while loop exited (user stop request)
        _ts("Stopped by user")
        _reset_pending()
        _ts("Drained stale CSV events from queue")
        _drain_queue(csv_event_queue)
        _pipeline_status.finish({"message": "Stopped by user"})

    worker = threading.Thread(target=_full_worker, daemon=True, name="full-pipeline-worker")
    worker.start()
    return {"ok": True, "message": "Full pipeline started (watchdog -> graph -> parquet -> grouper -> classifier -> DB)"}


def stop_monitor():
    """Request graceful stop of monitor loop and watchdogs."""
    global _stop_monitor
    _stop_monitor.set()
    _stop_watchdogs()
    # Immediately mark as finished so Start Monitor can be re-pressed
    _pipeline_status.finish({"message": "Stopped by user"})
    return {"ok": True, "message": "Stopped by user"}


def stop_batch_pipeline():
    """Request graceful stop of batch pipeline."""
    global _stop_batch
    _stop_batch.set()
    return {"ok": True, "message": "Batch stop requested"}
