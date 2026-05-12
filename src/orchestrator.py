"""
SRF Postmortem Orchestrator — bridges pipeline modules with WebDB.

Combines:
1. Monitoring pipeline: Preprocessor → Grouper → Classifier → Visualizer → Reporter → EmailSender
2. WebDB pipeline: Parquet Import → DB Classification → Similarity → Web Server
"""
import sys
import json
from pathlib import Path

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

    Supports two modes:
    - batch: Process all files through full pipeline -> DB
    - monitor: Watch folders, process new files, import to DB
    - web: Start web server with DB
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

    def run_classifier(self) -> tuple:
        """Step 3: Classify merged events (monitoring pipeline style)."""
        logger.info(f"Classifier: Starting classification for {len(list(self.paths.merged_dir.glob('*.parquet')))} files")
        df = self.classifier.run(
            input_dir=str(self.paths.merged_dir),
            output_dir=str(self.paths.results_dir),
        )
        logger.info(f"Classifier: {len(df)} events classified")
        self._save_per_event_classification()
        return df

    def run_visualizer(self) -> list:
        """Step 4: Generate visualization graphs."""
        merged_files = list(self.paths.merged_dir.glob("*.parquet"))
        graph_files = []
        for merged_file in merged_files:
            wide = self.paths.graphs_dir / f"{merged_file.stem}_wide_el.jpg"
            if self.visualizer.plot_single(str(merged_file), str(wide), time_range='wide', style='event_labeller'):
                graph_files.append(str(wide))
            narrow = self.paths.graphs_dir / f"{merged_file.stem}_narrow_el.jpg"
            if self.visualizer.plot_single(str(merged_file), str(narrow), time_range='narrow', style='event_labeller'):
                graph_files.append(str(narrow))
        logger.info(f"Visualizer: {len(graph_files)} graphs generated")
        return graph_files

    def run_reporter(self) -> list:
        """Step 5: Generate text reports."""
        merged_files = list(self.paths.merged_dir.glob("*.parquet"))
        report_files = []
        for merged_file in merged_files:
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

    def run_email_sender(self) -> list:
        """Step 6: Send email reports."""
        if not self.email_sender:
            logger.info("Email sender not configured — skipping")
            return []
        merged_files = list(self.paths.merged_dir.glob("*.parquet"))
        results = []
        for merged_file in merged_files:
            graphs = list(self.paths.graphs_dir.glob(f"{merged_file.stem}_*.jpg"))
            report_file = self.paths.reports_dir / f"{merged_file.stem}_report.md"
            cls_file = self.paths.results_dir / f"{merged_file.stem}_classification.json"

            if report_file.exists() and graphs:
                summary = "Unclassified"
                if cls_file.exists():
                    summary = json.load(open(cls_file)).get('description', summary)
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

    def backup_database(self) -> str:
        """Create a full backup of the database AND merged data."""
        from datetime import datetime
        import shutil
        import tarfile
        
        db_path = Path(self.config.db.path)
        merged_dir = Path(self.paths.merged_dir)
        # attachments는 projects root/data/attachments/
        attachments_dir = db_path.parent.parent / "data" / "attachments"

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

        return str(backup_filename)

    def restore_database(self, backup_file: str) -> dict:
        """Restore from a full backup tar.gz.
        
        Extracts: events.db, merged/, attachments/
        Returns dict with status and details.
        """
        import shutil
        import tarfile
        import tempfile
        
        backup_path = Path(backup_file)
        db_path = Path(self.config.db.path)
        merged_dir = Path(self.paths.merged_dir)
        attachments_dir = db_path.parent.parent / "data" / "attachments"
        
        if not backup_path.exists():
            return {"ok": False, "error": f"Backup file not found: {backup_file}"}
        
        # Extract to temp dir
        tmp_dir = Path(tempfile.mkdtemp(prefix="srf_restore_"))
        try:
            with tarfile.open(backup_path, "r:gz") as tar:
                tar.extractall(path=tmp_dir)
            
            restored = []
            
            # Restore DB
            src_db = tmp_dir / "events.db"
            if src_db.exists():
                # Backup current DB first
                current_bak = db_path.with_suffix(".db.restore_bak")
                if db_path.exists():
                    shutil.copy2(db_path, current_bak)
                shutil.copy2(src_db, db_path)
                restored.append("events.db")
            
            # Restore merged parquet
            src_merged = tmp_dir / "merged"
            if src_merged.exists():
                merged_dir.mkdir(parents=True, exist_ok=True)
                for f in src_merged.iterdir():
                    if f.name.endswith(":Zone.Identifier"):
                        continue
                    if f.suffix == ".parquet":
                        shutil.copy2(f, merged_dir / f.name)
                restored.append("merged/")
            
            # Restore attachments
            src_attachments = tmp_dir / "attachments"
            if src_attachments.exists():
                attachments_dir.mkdir(parents=True, exist_ok=True)
                for event_dir in src_attachments.iterdir():
                    if event_dir.is_dir():
                        target = attachments_dir / event_dir.name
                        target.mkdir(parents=True, exist_ok=True)
                        for f in event_dir.iterdir():
                            shutil.copy2(f, target / f.name)
                restored.append("attachments/")
            
            return {"ok": True, "restored": restored}
        except Exception as e:
            return {"ok": False, "error": str(e)}
        finally:
            # Cleanup temp
            shutil.rmtree(tmp_dir, ignore_errors=True)

    def _save_per_event_classification(self):
        """Extract per-event classification JSON from sequence_info.json."""
        seq_file = self.paths.results_dir / "sequence_info.json"
        if not seq_file.exists():
            return
        data = json.loads(seq_file.read_text())
        count = 0
        for fname, info in data.items():
            cls = info.get("classification")
            if cls:
                out = self.paths.results_dir / f"{Path(fname).stem}_classification.json"
                out.write_text(json.dumps(cls, indent=2))
                count += 1
        if count:
            logger.info(f"Created {count} per-event classification JSON files")

    def run_full_pipeline(self, input_dirs=None, incremental=False):
        """Run the complete monitoring pipeline (Steps 1-6)."""
        self.setup_directories()
        
        # 1. Preprocessor (already handles skipping)
        targets = input_dirs if input_dirs else self.config.paths.watch_folders
        preprocessor_result = self.run_preprocessor(targets)
        
        # If no new files processed and incremental=True, stop early
        if incremental and preprocessor_result['stats']['success'] == 0:
            logger.info("Incremental pipeline: No new files to process.")
            return

        # 2. Identify new files if incremental
        new_files = []
        if incremental:
            # Detect what was freshly processed
            for i in range(1, 4):
                scope_dir = self.paths.processed_dir / f"scope{i}"
                # Get files processed in the last 'check_interval' or matching recent timestamps
                # For simplicity, we filter by files that don't have a corresponding merged entry yet
                pass # Logic to be refined
        
        self.run_grouper()
        self.run_classifier()
        self.run_visualizer()
        self.run_reporter()
        self.run_email_sender()

    def run_batch(self, db_import=False):
        """Batch mode: process all files and optionally import to DB."""
        self.run_full_pipeline()
        if db_import:
            self.import_to_db()

    def run_monitor(self):
        """Continuous monitoring mode (from original main.py)."""
        self.setup_directories()

        import time
        from datetime import datetime

        # Check watch folders exist
        for wf in self.paths.watch_folders:
            if not wf.exists():
                raise FileNotFoundError(f"Watch folder not found: {wf}")

        # Track processed files
        processed_csv = {wf: set(f.name for f in wf.glob("*.csv")) for wf in self.paths.watch_folders}
        processed_parquet = {}
        for i in range(1, 4):
            scope_key = f"scope{i}"
            sd = self.paths.processed_dir / scope_key
            processed_parquet[scope_key] = set(f.name for f in sd.glob("*.parquet")) if sd.exists() else set()

        logger.info(f"Monitoring {len(self.paths.watch_folders)} folders")

        try:
            while True:
                # Check for new CSV files per scope
                for i, wf in enumerate(self.paths.watch_folders, 1):
                    csvs = [f for f in wf.glob("*.csv") if f.name not in processed_csv[wf]]
                    if not csvs:
                        continue
                    scope_dir = self.paths.processed_dir / f"scope{i}"
                    scope_dir.mkdir(parents=True, exist_ok=True)
                    for csv_path in csvs:
                        parquet_path = scope_dir / f"{csv_path.stem}.parquet"
                        self.preprocessor.process_one(csv_path, parquet_path)
                        processed_csv[wf].add(csv_path.name)

                # Check if all 3 scopes have new parquet files
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

        if mode == "batch":
            self.run_batch(db_import=True)
        elif mode == "monitor":
            self.run_monitor()
        elif mode == "web":
            self.run_web_server()
        elif mode == "full":
            self.run_batch(db_import=True)
            self.run_web_server()
        else:
            raise ValueError(f"Unknown mode: {mode}")
