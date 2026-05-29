"""
SRF Postmortem Orchestrator — bridges pipeline modules with WebDB.

4 Pipeline Paths:
1. Full Pipeline  : watch folder 3개 모든 csv → preprocess → merge → classify → visualizer → DB
2. Restore         : 백업 tar.gz에서 DB + merged parquet 복원, 경로 재연결
3. Append Pipeline : data/append/scope{1,2,3} csv → preprocess → merge → classify → DB
4. Monitor Trigger : watch folder 새 csv 3개만 → preprocess → merge → classify → DB

Combines:
- Monitoring pipeline: Preprocessor → Grouper → Classifier → Visualizer → Reporter → EmailSender
- WebDB pipeline: Parquet Import → DB Classification → Similarity → Web Server
"""
import sys
import json
import sqlite3
from pathlib import Path
from typing import Optional

import pandas as pd

# Add parent to path for commands
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.core.config import get_config, AppConfig
from src.core.logger import get_logger
from src.pipeline.preprocessor import Preprocessor
from src.pipeline.grouper import Grouper
from src.pipeline.classifier import AcceleratorEventClassifier as PipelineClassifier
from src.pipeline.visualizer import Visualizer
from src.pipeline.reporter import ReportGenerator
from src.pipeline.email_sender import EmailSender
from src.db.schema import init_db_sync, get_sync_connection
from src.db.repository import create_event, get_event, list_events
from src.classifier.classifier import classify_event

logger = get_logger(__name__)


class SRFOrchestrator:
    """
    Unified orchestrator for the SRF Postmortem system.

    Four pipeline paths:
      1. run_full_pipeline()  — 모든 watch folder csv 처리 → merge → classify → visualizer → DB
      2. restore_database()   — 백업 복원 + merged_file 경로 재연결 + visualizer 재생성
      3. run_append_pipeline()— data/append/scope csv 처리 → merge → classify → DB
      4. run_monitor()        — watch folder 새 파일 감지 → 바로 처리 (기존 monitor)
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.paths = config.paths
        self.system = config.system

        # Monitoring pipeline modules
        self.preprocessor = Preprocessor(config)
        self.classifier = PipelineClassifier(config=config)
        self.visualizer = Visualizer(config)
        self.reporter = ReportGenerator(config)
        try:
            self.email_sender = EmailSender(config.email)
        except (ValueError, Exception):
            logger.warning("Email sender not configured — email reports disabled")
            self.email_sender = None

        logger.info("SRF Orchestrator initialized")

    # ── Path 1: Full Pipeline ──────────────────────────────────

    def run_full_pipeline(self, db_import=True):
        """Path 1: 모든 watch folder csv 파일을 처리 → merge → classify → visualizer → DB.
        
        Args:
            db_import: DB import 여부 (기본 True)
        """
        logger.info("=== Path 1: Full Pipeline Start ===")
        self.setup_directories()
        self.run_preprocessor()
        self.run_grouper()
        self.run_classifier()
        self.run_visualizer()
        self.run_reporter()
        self.run_email_sender()
        if db_import:
            self.import_to_db()
        logger.info("=== Path 1: Full Pipeline Complete ===")

    def run_batch(self, db_import=False):
        """Legacy: run_full_pipeline alias for backward compatibility."""
        return self.run_full_pipeline(db_import=db_import)

    # ── Path 3: Append Pipeline ───────────────────────────────

    def run_append_pipeline(self, append_dirs: list = None, db_import=True):
        """Path 3: data/append/scope{1,2,3} csv 파일 → preprocess → merge → classify → DB.
        
        Args:
            append_dirs: [scope1_dir, scope2_dir, scope3_dir] (None = config.paths.append_dirs)
            db_import: DB import 여부 (기본 True)
        """
        logger.info("=== Path 3: Append Pipeline Start ===")
        if append_dirs is None:
            append_dirs = list(self.paths.append_dirs)
        if not append_dirs or len(append_dirs) != 3:
            raise ValueError("Exactly 3 append directories required: [scope1, scope2, scope3]")
        self.setup_directories()
        self.run_preprocessor(append_dirs)
        self.run_grouper()
        self.run_classifier()
        self.run_visualizer()
        self.run_reporter()
        self.run_email_sender()
        if db_import:
            self.import_to_db()
        logger.info("=== Path 3: Append Pipeline Complete ===")

    # ── Common Pipeline Steps ──────────────────────────────────

    def setup_directories(self) -> None:
        """Create all necessary directories."""
        dirs = [
            self.paths.processed_dir,
            self.paths.merged_dir,
            self.paths.results_dir,
            self.paths.reports_dir,
            self.paths.graphs_dir,
            self.paths.log_dir,
        ]
        for directory in dirs:
            directory.mkdir(parents=True, exist_ok=True)

    def run_preprocessor(self, input_dirs=None) -> dict:
        """Step 1: Preprocess CSV files into parquet."""
        result = {"stats": {"success": 0, "skipped": 0, "total": 0, "errors": 0}}
        if input_dirs:
            for i, input_dir in enumerate(input_dirs, 1):
                scope_output = self.paths.processed_dir / f"scope{i}"
                result = self.preprocessor.process_folder(Path(input_dir), scope_output)
                logger.info(f"Processed scope{i}: {result['stats']['success']} success, {result['stats']['skipped']} skipped")
            return result

        for i, watch_folder in enumerate(self.paths.watch_folders, 1):
            if not watch_folder.exists():
                logger.warning(f"Watch folder does not exist: {watch_folder}")
                continue
            scope_output = self.paths.processed_dir / f"scope{i}"
            result = self.preprocessor.process_folder(watch_folder, scope_output)
            logger.info(f"Processed scope{i}: {result['stats']['success']} success, {result['stats']['skipped']} skipped")
        return result

    def run_grouper(self) -> dict:
        """Step 2: Group events from scopes into merged parquet files."""
        grouper = Grouper(
            input_dirs=[
                self.paths.processed_dir / "scope1",
                self.paths.processed_dir / "scope2",
                self.paths.processed_dir / "scope3",
            ],
            output_dir=self.paths.merged_dir,
            window_s=self.config.grouper.window_s,
        )
        result = grouper.run()
        logger.info(f"Grouper: {result.get('matched_events', 0)} events merged")
        return result

    def _resolve_merged_files(self, merged_files: Optional[list] = None) -> list:
        """Resolve merged files: use provided list, or scan merged_dir if None."""
        if merged_files is not None:
            return list(merged_files)
        return sorted(self.paths.merged_dir.glob("*.parquet"))

    def run_classifier(self, merged_files: Optional[list] = None) -> tuple:
        """Step 3: Classify merged events.

        Args:
            merged_files: Specific files to classify (None = all in merged_dir).
        """
        files = self._resolve_merged_files(merged_files)
        if not files:
            logger.info("Classifier: No files to classify — skipping")
            return pd.DataFrame()

        # 새 파일만 temp dir에 복사해서 classifier에 전달 (merged_dir 전체를 긁지 않도록)
        import tempfile
        import shutil
        temp_input = Path(tempfile.mkdtemp(prefix="srf_cls_"))
        try:
            for f in files:
                shutil.copy2(f, temp_input / f.name)
            logger.info(f"Classifier: Classifying {len(files)} file(s)")
            df = self.classifier.run(
                input_dir=str(temp_input),
                output_dir=str(self.paths.results_dir),
            )
        finally:
            shutil.rmtree(temp_input, ignore_errors=True)

        if len(df) > 0:
            logger.info(f"Classifier: {len(df)} events classified")
        self._save_per_event_classification()
        return df

    def run_visualizer(self, merged_files: Optional[list] = None) -> list:
        """Step 4: Generate visualization graphs.

        Args:
            merged_files: Specific files to visualize (None = all in merged_dir).
        """
        files = self._resolve_merged_files(merged_files)
        graph_files = []
        for merged_file in files:
            wide = self.paths.graphs_dir / f"{merged_file.stem}_wide_el.jpg"
            if self.visualizer.plot_single(str(merged_file), str(wide), time_range='wide', style='event_labeller'):
                graph_files.append(str(wide))
            narrow = self.paths.graphs_dir / f"{merged_file.stem}_narrow_el.jpg"
            if self.visualizer.plot_single(str(merged_file), str(narrow), time_range='narrow', style='event_labeller'):
                graph_files.append(str(narrow))
        logger.info(f"Visualizer: {len(graph_files)} graphs generated")
        return graph_files

    def run_reporter(self, merged_files: Optional[list] = None) -> list:
        """Step 5: Generate text reports.

        Args:
            merged_files: Specific files to report (None = all in merged_dir).
        """
        files = self._resolve_merged_files(merged_files)
        report_files = []
        for merged_file in files:
            cls_file = self.paths.results_dir / f"{merged_file.stem}_classification.json"
            if cls_file.exists():
                report = self.reporter.generate(
                    parquet_file=merged_file,
                    classification_file=cls_file,
                    output_format='markdown',
                    output_path=self.paths.reports_dir / f"{merged_file.stem}_report.md",
                )
                report_files.append(str(report))
        logger.info(f"Reporter: {len(report_files)} reports generated")
        return report_files

    def run_email_sender(self, merged_files: Optional[list] = None) -> list:
        """Step 6: Send email reports.

        Args:
            merged_files: Specific files to email (None = all in merged_dir).
        """
        if not self.email_sender:
            logger.info("Email sender not configured — skipping")
            return []
        files = self._resolve_merged_files(merged_files)
        results = []
        for merged_file in files:
            graphs = list(self.paths.graphs_dir.glob(f"{merged_file.stem}_*.jpg"))
            report_file = self.paths.reports_dir / f"{merged_file.stem}_report.md"
            cls_file = self.paths.results_dir / f"{merged_file.stem}_classification.json"

            if report_file.exists() and graphs:
                summary = "Unclassified"
                if cls_file.exists():
                    summary = json.loads(cls_file.read_text(encoding='utf-8')).get('description', summary)
                ok = self.email_sender.send_report(
                    report_content=report_file.read_text(encoding='utf-8'),
                    report_format='markdown',
                    graph_files=[str(g) for g in graphs],
                    classification_summary=summary,
                    to=self.config.email.receiver_emails,
                )
                results.append({"event": merged_file.name, "success": ok})
        logger.info(f"Email sender: {sum(r['success'] for r in results)} sent")
        return results

    def import_to_db(self) -> int:
        """Import merged parquet files into the WebDB database."""
        from src.import_job import run_import
        cfg_path = None
        try:
            from src.core.config import get_config_path
            cfg_path = get_config_path()
        except Exception:
            pass
        result = run_import(self.paths.merged_dir, cfg_path)
        count = result.get('imported', 0)
        logger.info(f"DB import: {count} imported, {result.get('skipped', 0)} skipped, {result.get('errors', 0)} errors")
        return count

    # ── Path 2: Backup & Restore ───────────────────────────────

    def backup_database(self, include_graphs: bool = False, include_results: bool = False) -> str:
        """Create a full backup of the database AND merged data.
        
        Args:
            include_graphs: graphs/ 디렉토리 포함 여부 (기본 False, 용량 큼)
            include_results: results/ 디렉토리 포함 여부 (기본 False)
        """
        from datetime import datetime
        import shutil
        import tarfile
        
        db_path = Path(self.config.db.path)
        merged_dir = Path(self.paths.merged_dir)
        graphs_dir = Path(self.paths.graphs_dir)
        results_dir = Path(self.paths.results_dir)
        reports_dir = Path(self.paths.reports_dir)
        # attachments: project_root/data/attachments/
        # Resolve from merged_dir: merged is at data/merged, so attachments is data/attachments/
        project_root = merged_dir.resolve().parent.parent if merged_dir else Path(self.config.db.path).resolve().parent.parent
        attachments_dir = project_root / "data" / "attachments"
        if not attachments_dir.exists():
            # Try alternate: use the same logic as web server _get_attachments_dir
            attachments_dir = Path(__file__).resolve().parent.parent / "data" / "attachments"

        if not db_path.exists():
            return "Database not found"

        backup_dir = db_path.parent / "backups"
        backup_dir.mkdir(parents=True, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = backup_dir / f"full_backup_{timestamp}.tar.gz"

        with tarfile.open(backup_filename, "w:gz") as tar:
            tar.add(db_path, arcname=db_path.name)
            if merged_dir.exists():
                tar.add(merged_dir, arcname=merged_dir.name)
            if attachments_dir.exists():
                tar.add(attachments_dir, arcname=attachments_dir.name)
            if include_graphs and graphs_dir.exists():
                tar.add(graphs_dir, arcname=graphs_dir.name)
            if include_results and results_dir.exists():
                tar.add(results_dir, arcname=results_dir.name)
            # reports는 용량 작으므로 항상 포함
            if reports_dir.exists():
                tar.add(reports_dir, arcname=reports_dir.name)

        return str(backup_filename)

    def restore_database(self, backup_file: str, regenerate_visuals: bool = True) -> dict:
        """Path 2: 백업 파일로 DB + merged parquet + attachments 복원.
        
        복원 후 merged_file 경로를 현재 프로젝트 위치로 재연결하고,
        필요시 graphs/results를 재생성합니다.
        
        Args:
            backup_file: 백업 tar.gz 경로
            regenerate_visuals: 복원 후 visualizer/reporter 재실행 여부
        """
        import shutil
        import tarfile
        import tempfile
        
        backup_path = Path(backup_file)
        db_path = Path(self.config.db.path).resolve()
        merged_dir = Path(self.paths.merged_dir).resolve()
        graphs_dir = Path(self.paths.graphs_dir).resolve()
        results_dir = Path(self.paths.results_dir).resolve()
        reports_dir = Path(self.paths.reports_dir).resolve()
        # Use same resolution logic as backup_database
        project_root = merged_dir.parent.parent if merged_dir else Path(__file__).resolve().parent.parent
        attachments_dir = project_root / "data" / "attachments"
        if not attachments_dir.exists():
            attachments_dir = Path(__file__).resolve().parent.parent / "data" / "attachments"
        
        if not backup_path.exists():
            return {"ok": False, "error": f"Backup file not found: {backup_file}"}
        
        # Extract to temp dir
        tmp_dir = Path(tempfile.mkdtemp(prefix="srf_restore_"))
        try:
            with tarfile.open(backup_path, "r:gz") as tar:
                # arcname이 디렉토리 구조를 유지하도록 함
                tar.extractall(path=tmp_dir)
            
            restored = []
            
            # ── 1. Restore DB ──
            src_db = tmp_dir / "events.db"
            if src_db.exists():
                current_bak = db_path.with_suffix(".db.restore_bak")
                if db_path.exists():
                    shutil.copy2(db_path, current_bak)
                    restored.append("backup_created")
                shutil.copy2(src_db, db_path)
                restored.append("events.db")
            else:
                # tar에 db 파일명이 다른 경우 찾기
                db_candidates = list(tmp_dir.glob("*.db")) + list(tmp_dir.glob("*.sqlite"))
                if db_candidates:
                    src_db = db_candidates[0]
                    current_bak = db_path.with_suffix(".db.restore_bak")
                    if db_path.exists():
                        shutil.copy2(db_path, current_bak)
                        restored.append("backup_created")
                    shutil.copy2(src_db, db_path)
                    restored.append(f"events.db (from {src_db.name})")
            
            # ── 2. Restore merged parquet ──
            src_merged = tmp_dir / "merged"
            if not src_merged.exists():
                # 상위 디렉토리에 있을 수도 있음
                for d in tmp_dir.iterdir():
                    if d.is_dir() and d.name in ("merged", "data"):
                        inner = d / "merged" if d.name == "data" else d
                        if inner.exists():
                            src_merged = inner
                            break

            if src_merged.exists():
                merged_dir.mkdir(parents=True, exist_ok=True)
                # backup의 merged 파일을 덮어쓰기
                copied = 0
                for f in src_merged.iterdir():
                    if f.name.endswith(":Zone.Identifier"):
                        continue
                    if f.suffix == ".parquet":
                        shutil.copy2(f, merged_dir / f.name)
                        copied += 1
                restored.append(f"merged/ ({copied} parquet files)")
            else:
                logger.warning("No merged parquet found in backup")

            # Update merged_file paths in DB to point to current location (quick pass)
            if src_db.exists():
                try:
                    cur_conn = sqlite3.connect(str(db_path))
                    new_merged_root = str(merged_dir.resolve())
                    cur_conn.execute(
                        """
                        UPDATE events
                        SET merged_file = ? || '/' || 'event_' || id || '.parquet'
                        WHERE merged_file IS NOT NULL
                        """,
                        (new_merged_root,),
                    )
                    updated_rows = cur_conn.rowcount
                    cur_conn.commit()
                    cur_conn.close()
                    if updated_rows > 0:
                        restored.append(f"merged_file paths updated ({updated_rows} rows)")
                except Exception as e:
                    logger.warning(f"Failed to update merged_file paths: {e}")

            # ── 3. Restore attachments ──
            src_attachments = tmp_dir / "attachments"
            if src_attachments.exists() and src_attachments.is_dir():
                attachments_dir.mkdir(parents=True, exist_ok=True)
                for event_dir in src_attachments.iterdir():
                    if event_dir.is_dir():
                        target = attachments_dir / event_dir.name
                        target.mkdir(parents=True, exist_ok=True)
                        for f in event_dir.iterdir():
                            shutil.copy2(f, target / f.name)
                restored.append("attachments/")
            
            # ── 4. Restore optional dirs from backup ──
            for src_name, dst_dir in [("graphs", graphs_dir), ("results", results_dir), ("reports", reports_dir)]:
                src_path = tmp_dir / src_name
                if src_path.exists() and src_path.is_dir():
                    dst_dir.mkdir(parents=True, exist_ok=True)
                    for f in src_path.iterdir():
                        if not f.name.endswith(":Zone.Identifier"):
                            shutil.copy2(f, dst_dir / f.name)
                    restored.append(f"{src_name}/")

            # ── 5. Update merged_file paths in DB ──
            merged_paths_updated = 0
            merged_paths_failed = 0
            if src_db.exists() or any(tmp_dir.glob("*.db")):
                try:
                    # DB 커넥션: 복원된 DB 사용 (캐시 방지를 위해 직접 열기)
                    cur_conn = sqlite3.connect(str(db_path))
                    cur_conn.execute("PRAGMA journal_mode=DELETE")
                    
                    new_merged_root = str(merged_dir)
                    cursor = cur_conn.execute(
                        """
                        UPDATE events
                        SET merged_file = ? || '/' || 'event_' || id || '.parquet'
                        WHERE merged_file IS NOT NULL
                        """,
                        (new_merged_root,),
                    )
                    merged_paths_updated = cursor.rowcount
                    cur_conn.commit()
                    
                    # Verify: 존재하지 않는 merged_file이 있는지 확인
                    cursor = cur_conn.execute(
                        "SELECT merged_file FROM events WHERE merged_file IS NOT NULL"
                    )
                    missing = []
                    for row in cursor.fetchall():
                        if not Path(row[0]).exists():
                            missing.append(row[0])
                    if missing:
                        logger.warning(f"{len(missing)} merged_file paths still unresolved after update")
                        for m in missing[:5]:
                            logger.warning(f"  Missing: {m}")
                    else:
                        restored.append(f"merged_file paths verified ({merged_paths_updated} rows)")
                    
                    cur_conn.close()
                except Exception as e:
                    merged_paths_failed = 1
                    logger.warning(f"Failed to update merged_file paths: {e}")
            
            if merged_paths_updated > 0:
                restored.append(f"merged_file paths updated ({merged_paths_updated} rows)")
            if merged_paths_failed:
                restored.append("merged_file path update FAILED")
            
            # ── 6. Regenerate visuals (graphs + classification) if requested ──
            if regenerate_visuals:
                merged_files = sorted(merged_dir.glob("*.parquet"))
                if merged_files:
                    logger.info(f"Regenerating visuals for {len(merged_files)} events...")
                    cls_files_before = len(list(results_dir.glob("*_classification.json")))
                    graph_files_before = len(list(graphs_dir.glob("*.jpg")))
                    
                    # Classification 재실행 (results/*_classification.json 있으면 skip)
                    self.run_classifier(merged_files=merged_files)
                    
                    # Graphs 재생성
                    self.run_visualizer(merged_files=merged_files)
                    
                    # Reports 재생성
                    self.run_reporter(merged_files=merged_files)
                    
                    cls_files_after = len(list(results_dir.glob("*_classification.json")))
                    graph_files_after = len(list(graphs_dir.glob("*.jpg")))
                    report_files_after = len(list(reports_dir.glob("*_report.md")))
                    
                    restored.append(
                        f"visuals regenerated: {cls_files_after} cls, "
                        f"{graph_files_after} graphs, {report_files_after} reports"
                    )
                else:
                    logger.warning("No merged parquet files to regenerate visuals for")
            
            return {"ok": True, "restored": restored}
        except Exception as e:
            import traceback
            logger.error(f"Restore failed: {e}\n{traceback.format_exc()}")
            return {"ok": False, "error": str(e)}
        finally:
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _save_per_event_classification(self):
        """Extract per-event classification JSON from sequence_info.json."""
        seq_file = self.paths.results_dir / "sequence_info.json"
        if not seq_file.exists():
            return
        data = json.loads(seq_file.read_text(encoding='utf-8'))
        count = 0
        for fname, info in data.items():
            cls = info.get("classification")
            if cls:
                out = self.paths.results_dir / f"{Path(fname).stem}_classification.json"
                out.write_text(json.dumps(cls, indent=2))
                count += 1
        if count:
            logger.info(f"Created {count} per-event classification JSON files")

    # ── Path 4: Monitor ────────────────────────────────────────

    def run_monitor(self):
        """Path 4: Continuous monitoring — watch folder에 새로운 csv 3개씩 감지하여 실시간 처리."""
        self.setup_directories()

        import time
        from datetime import datetime

        for wf in self.paths.watch_folders:
            if not wf.exists():
                raise FileNotFoundError(f"Watch folder not found: {wf}")

        processed_csv = {wf: set(f.name for f in wf.glob("*.csv")) for wf in self.paths.watch_folders}
        processed_parquet = {}
        for i in range(1, 4):
            scope_key = f"scope{i}"
            sd = self.paths.processed_dir / scope_key
            processed_parquet[scope_key] = set(f.name for f in sd.glob("*.parquet")) if sd.exists() else set()

        logger.info(f"Monitoring {len(self.paths.watch_folders)} folders")

        try:
            while True:
                for i, wf in enumerate(self.paths.watch_folders, 1):
                    csvs = [f for f in wf.glob("*.csv") if f.name not in processed_csv[wf]]
                    if not csvs:
                        continue
                    scope_dir = self.paths.processed_dir / f"scope{i}"
                    scope_dir.mkdir(parents=True, exist_ok=True)
                    for csv_path in csvs:
                        for _ in range(15):
                            try:
                                with open(csv_path, 'rb') as __f:
                                    __f.read(1)
                                break
                            except (PermissionError, OSError):
                                time.sleep(1.0)
                        parquet_path = scope_dir / f"{csv_path.stem}.parquet"
                        self.preprocessor.process_one(csv_path, parquet_path)
                        processed_csv[wf].add(csv_path.name)

                all_new = True
                for i in range(1, 4):
                    sk = f"scope{i}"
                    current = set(f.name for f in (self.paths.processed_dir / sk).glob("*.parquet"))
                    if not (current - processed_parquet[sk]):
                        all_new = False
                        break

                if all_new:
                    logger.info("All 3 scopes have new data. Running pipeline...")
                    self.run_grouper()
                    self.run_classifier()
                    self.run_visualizer()
                    self.run_reporter()
                    self.run_email_sender()
                    self.import_to_db()
                    for i in range(1, 4):
                        sk = f"scope{i}"
                        processed_parquet[sk] = set(f.name for f in (self.paths.processed_dir / sk).glob("*.parquet"))

                time.sleep(self.system.check_interval)
        except KeyboardInterrupt:
            logger.info("Monitor stopped")

    # ── Web Server ─────────────────────────────────────────────

    def run_web_server(self):
        """Start the WebDB web server."""
        from src.web.server import app
        import uvicorn
        host = getattr(self.config.web, 'host', '0.0.0.0')
        port = getattr(self.config.web, 'port', 8050)
        logger.info(f"Starting web server on {host}:{port}")
        uvicorn.run(app, host=host, port=port)

    def run(self, mode=None):
        """Run orchestrator in specified mode."""
        if mode is None:
            mode = self.system.mode

        if mode in ("batch", "full_pipeline"):
            self.run_full_pipeline(db_import=True)
        elif mode == "monitor":
            self.run_monitor()
        elif mode == "web":
            self.run_web_server()
        elif mode == "full":
            self.run_full_pipeline(db_import=True)
            self.run_web_server()
        elif mode == "append":
            self.run_append_pipeline(db_import=True)
        else:
            raise ValueError(f"Unknown mode: {mode}")
