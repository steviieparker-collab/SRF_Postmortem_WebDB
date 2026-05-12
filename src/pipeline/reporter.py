"""
Reporter module for SRF Event Monitoring System.

Generates comprehensive text analysis reports for SRF events by combining
classification results, signal metrics, and event details into structured
reports for email and documentation.
"""

from __future__ import annotations

import argparse
import json
import logging
import warnings
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
import pandas as pd
import numpy as np

# Core modules
from ..core.config import get_config
from ..core.logger import get_logger
from ..core.utils import (
    classify_columns,
    safe_read_parquet,
    load_json,
    save_json,
    ensure_directory,
)

# Pipeline datatypes
from ..pipeline.datatypes import SignalEvent, TimeGroup


# -----------------------------------------------------------------------------
# Data structures for report data
# -----------------------------------------------------------------------------

@dataclass
class EventMetrics:
    """Metrics extracted from parquet file for a single event."""
    filename: str
    timestamp: Optional[datetime] = None
    analog_channels: List[str] = field(default_factory=list)
    digital_channels: List[str] = field(default_factory=list)
    analog_stats: Dict[str, Dict[str, float]] = field(default_factory=dict)
    digital_counts: Dict[str, int] = field(default_factory=dict)
    baseline_values: Dict[str, float] = field(default_factory=dict)
    baseline_stds: Dict[str, float] = field(default_factory=dict)
    time_range: Tuple[float, float] = (0.0, 0.0)
    sampling_rate: float = 0.0


@dataclass
class ClassificationResult:
    """Classification results for an event."""
    case: int
    case_str: str
    description: str
    fault: str
    confidence: float
    first_group_count: int
    second_group_count: int
    third_group_count: int
    first_events: List[Dict[str, Any]] = field(default_factory=list)
    second_events: List[Dict[str, Any]] = field(default_factory=list)
    third_events: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class ReportData:
    """Complete data for a single event report."""
    event_metrics: EventMetrics
    classification: ClassificationResult
    config: Dict[str, Any]
    attachments: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.now)


# -----------------------------------------------------------------------------
# Data extractor
# -----------------------------------------------------------------------------

class DataExtractor:
    """Extract metrics from parquet files and classification results."""

    def __init__(self, config=None):
        if config is None:
            config = get_config()
        self.config = config
        self.classification_config = config.classification
        self.logger = get_logger(__name__)

    def extract_event_metrics(self, parquet_path: Path) -> EventMetrics:
        """Extract signal metrics from a parquet file."""
        self.logger.info(f"Extracting metrics from {parquet_path.name}")
        df = safe_read_parquet(parquet_path)

        time_col = "t_rel_s" if "t_rel_s" in df.columns else df.columns[0]
        analog_cols, digital_cols = classify_columns(df.columns.tolist(), df)

        timestamp = None
        if "event_timestamp" in df.columns:
            ts_val = df["event_timestamp"].iloc[0]
            if hasattr(ts_val, "timestamp"):
                timestamp = datetime.fromtimestamp(ts_val.timestamp())
            elif isinstance(ts_val, (int, float)):
                timestamp = datetime.fromtimestamp(ts_val)

        analog_stats = {}
        baseline_values = {}
        baseline_stds = {}

        for channel in analog_cols:
            if channel not in df.columns:
                continue
            values = df[channel].dropna().astype(float)
            if len(values) == 0:
                continue
            analog_stats[channel] = {
                "mean": float(values.mean()),
                "std": float(values.std()),
                "min": float(values.min()),
                "max": float(values.max()),
                "median": float(values.median()),
            }
            baseline_start = self.classification_config.baseline_start_s
            baseline_end = self.classification_config.baseline_end_s
            baseline_mask = (df[time_col] >= baseline_start) & (df[time_col] <= baseline_end)
            if baseline_mask.any():
                baseline_vals = df.loc[baseline_mask, channel].dropna()
                if len(baseline_vals) > 0:
                    baseline_values[channel] = float(baseline_vals.mean())
                    baseline_stds[channel] = float(baseline_vals.std())

        digital_counts = {}
        for channel in digital_cols:
            if channel not in df.columns:
                continue
            values = df[channel].dropna()
            digital_counts[channel] = int((values > 0.5).sum())

        t = df[time_col].astype(float)
        time_range = (float(t.min()), float(t.max()))

        if len(t) > 1:
            sampling_rate = 1.0 / np.mean(np.diff(t))
        else:
            sampling_rate = 0.0

        return EventMetrics(
            filename=parquet_path.name,
            timestamp=timestamp,
            analog_channels=analog_cols,
            digital_channels=digital_cols,
            analog_stats=analog_stats,
            digital_counts=digital_counts,
            baseline_values=baseline_values,
            baseline_stds=baseline_stds,
            time_range=time_range,
            sampling_rate=sampling_rate,
        )

    def extract_classification_result(self, results_dir: Path, filename: str) -> Optional[ClassificationResult]:
        """Extract classification results for a specific file."""
        csv_path = results_dir / "clustering_results.csv"
        if csv_path.exists():
            self.logger.debug(f"Looking for classification result for '{filename}' in {csv_path}")
            df = pd.read_csv(csv_path)
            row = df[df["filename"] == filename]
            if not row.empty:
                case = int(row["case"].iloc[0])
                case_str = str(row["case_str"].iloc[0]) if "case_str" in row.columns else str(case)
                description = row["description"].iloc[0]
                fault = row["fault"].iloc[0] if "fault" in row.columns else ""
                confidence = float(row["confidence"].iloc[0]) if "confidence" in row.columns else 0.0
                first_group = int(row["first_group_count"].iloc[0]) if "first_group_count" in row.columns else 0
                second_group = int(row["second_group_count"].iloc[0]) if "second_group_count" in row.columns else 0
                third_group = int(row["third_group_count"].iloc[0]) if "third_group_count" in row.columns else 0

                seq_path = results_dir / "sequence_info.json"
                first_events, second_events, third_events = [], [], []
                if seq_path.exists():
                    seq_data = load_json(seq_path)
                    file_key = filename
                    if file_key in seq_data:
                        seq = seq_data[file_key]
                        first_events = seq.get("first_events", [])
                        second_events = seq.get("second_events", [])
                        third_events = seq.get("third_events", [])

                return ClassificationResult(
                    case=case, case_str=case_str, description=description, fault=fault,
                    confidence=confidence, first_group_count=first_group,
                    second_group_count=second_group, third_group_count=third_group,
                    first_events=first_events, second_events=second_events, third_events=third_events,
                )

        self.logger.warning(f"No classification result found for {filename}")
        return None


# -----------------------------------------------------------------------------
# Template renderer (Jinja2)
# -----------------------------------------------------------------------------

try:
    from jinja2 import Environment, FileSystemLoader, select_autoescape
    JINJA2_AVAILABLE = True
except ImportError:
    JINJA2_AVAILABLE = False
    warnings.warn("Jinja2 not installed, template rendering disabled.")


class TemplateRenderer:
    """Render reports using Jinja2 templates."""

    def __init__(self, template_dir: Optional[Path] = None):
        if template_dir is None:
            # Adjust path to point to SRF_postmortem/src/templates/report
            template_dir = Path(__file__).parent.parent.parent / "templates" / "report"
        self.template_dir = template_dir

        if JINJA2_AVAILABLE:
            self.env = Environment(
                loader=FileSystemLoader(str(template_dir)),
                autoescape=select_autoescape(["html", "xml"]),
                trim_blocks=True,
                lstrip_blocks=True,
            )
        else:
            self.env = None

    def render(self, template_name: str, data: Dict[str, Any]) -> str:
        if self.env is None:
            raise RuntimeError("Jinja2 not installed. Install with 'pip install Jinja2'.")
        template = self.env.get_template(template_name)
        return template.render(**data)

    def render_report(self, report_data: ReportData, format: str = "markdown") -> str:
        data_dict = self._prepare_template_data(report_data)
        template_map = {
            "markdown": "email_body.md.j2",
            "html": "report.html.j2",
            "plaintext": "summary.txt.j2",
        }
        if format not in template_map:
            raise ValueError(f"Unsupported format: {format}. Choose from {list(template_map.keys())}")
        template_name = template_map[format]
        return self.render(template_name, data_dict)

    def _prepare_template_data(self, report_data: ReportData) -> Dict[str, Any]:
        event = report_data.event_metrics
        classification = report_data.classification

        confidence_pct = f"{classification.confidence * 100:.1f}%"
        timestamp_str = event.timestamp.strftime("%Y-%m-%d %H:%M:%S") if event.timestamp else "Unknown"

        analog_stats = []
        for channel in event.analog_channels:
            if channel in event.analog_stats:
                stats = event.analog_stats[channel]
                baseline = event.baseline_values.get(channel, 0.0)
                analog_stats.append({
                    "channel": channel, "mean": stats["mean"], "std": stats["std"],
                    "min": stats["min"], "max": stats["max"], "baseline": baseline,
                })

        digital_counts = []
        for channel in event.digital_channels:
            count = event.digital_counts.get(channel, 0)
            digital_counts.append({"channel": channel, "count": count})

        timeline = []
        for group_name, events in [
            ("FIRST", classification.first_events),
            ("SECOND", classification.second_events),
            ("THIRD", classification.third_events),
        ]:
            for ev in events:
                timeline.append({
                    "group": group_name, "channel": ev.get("channel", ""),
                    "time_raw": ev.get("time_raw", 0.0),
                    "time_effective": ev.get("time_effective", 0.0),
                    "type": ev.get("type", ""), "value": ev.get("value", 0.0),
                })
        timeline.sort(key=lambda x: x["time_effective"])

        return {
            "event": event, "classification": classification, "config": report_data.config,
            "attachments": report_data.attachments, "generated_at": report_data.generated_at,
            "confidence_pct": confidence_pct, "timestamp_str": timestamp_str,
            "analog_stats": analog_stats, "digital_counts": digital_counts,
            "timeline": timeline, "has_analog": len(analog_stats) > 0,
            "has_digital": len(digital_counts) > 0, "has_timeline": len(timeline) > 0,
        }


# -----------------------------------------------------------------------------
# Format converter
# -----------------------------------------------------------------------------

class FormatConverter:
    """Convert between different report formats."""

    @staticmethod
    def markdown_to_html(markdown: str) -> str:
        import re
        html = markdown
        html = re.sub(r'^# (.+)$', r'<h1>\1</h1>', html, flags=re.MULTILINE)
        html = re.sub(r'^## (.+)$', r'<h2>\1</h2>', html, flags=re.MULTILINE)
        html = re.sub(r'^### (.+)$', r'<h3>\1</h3>', html, flags=re.MULTILINE)
        html = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', html)
        html = re.sub(r'\*(.+?)\*', r'<em>\1</em>', html)
        html = re.sub(r'^\- (.+)$', r'<li>\1</li>', html, flags=re.MULTILINE)
        html = re.sub(r'(<li>.+</li>\n)+', r'<ul>\g<0></ul>', html)
        html = html.replace('\n', '<br>\n')
        return html

    @staticmethod
    def html_to_plaintext(html: str) -> str:
        import re
        text = html
        text = re.sub(r'<[^>]+>', '', text)
        text = text.replace('&nbsp;', ' ').replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = re.sub(r'\s+', ' ', text)
        return text.strip()


# -----------------------------------------------------------------------------
# Main report generator
# -----------------------------------------------------------------------------

class ReportGenerator:
    """Main orchestrator for report generation."""

    def __init__(self, config=None):
        from ..core.config import AppConfig, ReportConfig

        if config is None:
            config = get_config()

        if isinstance(config, AppConfig):
            self.config = config
            self.report_config = config.report
        elif isinstance(config, ReportConfig):
            self.report_config = config
            self.config = get_config()
        elif isinstance(config, dict):
            self.report_config = ReportConfig(**config)
            self.config = get_config()
        else:
            self.config = config
            self.report_config = config.report

        self.extractor = DataExtractor(self.config)
        self.renderer = TemplateRenderer()
        self.logger = get_logger(__name__)

    def generate_report(
        self,
        parquet_path: Path,
        results_dir: Path,
        output_path: Optional[Path] = None,
        format: str = "markdown",
        sections: Optional[List[str]] = None,
    ) -> str:
        """Generate a report for a single event."""
        self.logger.info(f"Generating report for {parquet_path.name}")

        event_metrics = self.extractor.extract_event_metrics(parquet_path)
        classification = self.extractor.extract_classification_result(results_dir, parquet_path.name)

        if classification is None:
            raise ValueError(f"No classification results found for {parquet_path.name}")

        if sections is None:
            sections = self.report_config.include_sections

        config_dict = {
            "sections": sections,
            "include_graphs": self.report_config.include_graphs,
            "include_raw_data": self.report_config.include_raw_data,
            "max_table_rows": self.report_config.max_table_rows,
            "truncate_long_values": self.report_config.truncate_long_values,
        }

        attachments = []
        if self.report_config.include_graphs:
            graphs_dir = self.config.paths.graphs_dir
            if graphs_dir.exists():
                pattern = f"{parquet_path.stem}_*.jpg"
                for graph_file in graphs_dir.glob(pattern):
                    attachments.append(str(graph_file.name))

        report_data = ReportData(
            event_metrics=event_metrics,
            classification=classification,
            config=config_dict,
            attachments=attachments,
        )

        if format == "json":
            import json
            result = json.dumps(self._report_data_to_dict(report_data), indent=2, ensure_ascii=False)
        else:
            result = self.renderer.render_report(report_data, format)

        if output_path is not None:
            output_path.parent.mkdir(parents=True, exist_ok=True)
            with open(output_path, "w", encoding="utf-8") as f:
                f.write(result)
            self.logger.info(f"Report saved to {output_path}")

        return result

    def generate(
        self,
        parquet_file: Union[str, Path],
        classification_file: Union[str, Path],
        output_format: str = "markdown",
        output_path: Optional[Union[str, Path]] = None,
    ) -> str:
        """
        Generate a report from parquet file and classification file.

        Simplified interface for single file processing.
        """
        from pathlib import Path
        import shutil
        import tempfile

        parquet_path = Path(parquet_file)
        classification_path = Path(classification_file)

        temp_dir = Path(tempfile.mkdtemp(prefix="srf_report_"))
        try:
            target_name = parquet_path.stem + "_classification.json"
            target_path = temp_dir / target_name
            shutil.copy2(classification_path, target_path)

            source_dir = classification_path.parent
            for extra in ["clustering_results.csv", "sequence_info.json", "event_points.json"]:
                src = source_dir / extra
                if src.exists():
                    shutil.copy2(src, temp_dir / extra)

            return self.generate_report(
                parquet_path=parquet_path,
                results_dir=temp_dir,
                output_path=Path(output_path) if output_path else None,
                format=output_format,
                sections=self.report_config.include_sections,
            )
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)

    def generate_batch_report(
        self,
        parquet_dir: Path,
        results_dir: Path,
        output_path: Path,
        format: str = "markdown",
        max_events: int = 10,
    ) -> str:
        """Generate a batch report summarizing multiple events."""
        self.logger.info(f"Generating batch report for up to {max_events} events")

        parquet_files = list(parquet_dir.glob("*.parquet"))
        if not parquet_files:
            raise ValueError(f"No parquet files found in {parquet_dir}")

        parquet_files = parquet_files[:max_events]

        events_data = []
        for parquet_path in parquet_files:
            try:
                event_metrics = self.extractor.extract_event_metrics(parquet_path)
                classification = self.extractor.extract_classification_result(results_dir, parquet_path.name)
                if classification is not None:
                    events_data.append((event_metrics, classification))
            except Exception as e:
                self.logger.warning(f"Failed to process {parquet_path.name}: {e}")

        if not events_data:
            raise ValueError("No valid events found for batch report")

        case_counts = {}
        for _, classification in events_data:
            case = classification.case_str
            case_counts[case] = case_counts.get(case, 0) + 1

        config_dict = {
            "sections": self.report_config.include_sections,
            "include_graphs": False,
            "include_raw_data": False,
        }

        template_data = {
            "events": [
                {
                    "filename": event_metrics.filename,
                    "timestamp": event_metrics.timestamp,
                    "case": classification.case_str,
                    "description": classification.description,
                    "confidence": classification.confidence,
                    "fault": classification.fault,
                }
                for event_metrics, classification in events_data
            ],
            "case_counts": case_counts,
            "total_events": len(events_data),
            "generated_at": datetime.now(),
            "config": config_dict,
        }

        template_name = f"batch_summary.{format}.j2"
        try:
            result = self.renderer.render(template_name, template_data)
        except Exception:
            result = self._generate_fallback_batch_report(events_data, case_counts)

        output_path.parent.mkdir(parents=True, exist_ok=True)
        with open(output_path, "w", encoding="utf-8") as f:
            f.write(result)

        self.logger.info(f"Batch report saved to {output_path}")
        return result

    def _report_data_to_dict(self, report_data: ReportData) -> Dict[str, Any]:
        return {
            "event_metrics": {
                "filename": report_data.event_metrics.filename,
                "timestamp": report_data.event_metrics.timestamp.isoformat() if report_data.event_metrics.timestamp else None,
                "analog_channels": report_data.event_metrics.analog_channels,
                "digital_channels": report_data.event_metrics.digital_channels,
                "analog_stats": report_data.event_metrics.analog_stats,
                "digital_counts": report_data.event_metrics.digital_counts,
                "baseline_values": report_data.event_metrics.baseline_values,
                "baseline_stds": report_data.event_metrics.baseline_stds,
                "time_range": report_data.event_metrics.time_range,
                "sampling_rate": report_data.event_metrics.sampling_rate,
            },
            "classification": {
                "case": report_data.classification.case,
                "case_str": report_data.classification.case_str,
                "description": report_data.classification.description,
                "fault": report_data.classification.fault,
                "confidence": report_data.classification.confidence,
                "first_group_count": report_data.classification.first_group_count,
                "second_group_count": report_data.classification.second_group_count,
                "third_group_count": report_data.classification.third_group_count,
                "first_events": report_data.classification.first_events,
                "second_events": report_data.classification.second_events,
                "third_events": report_data.classification.third_events,
            },
            "config": report_data.config,
            "attachments": report_data.attachments,
            "generated_at": report_data.generated_at.isoformat(),
        }

    def _generate_fallback_batch_report(self, events_data, case_counts) -> str:
        lines = ["SRF Event Batch Summary", "=" * 50, ""]
        lines.append(f"Total events: {len(events_data)}")
        lines.append(f"Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append("")
        lines.append("Case Distribution:")
        for case, count in sorted(case_counts.items()):
            lines.append(f"  Case {case}: {count} events")
        lines.append("")
        lines.append("Event Details:")
        for i, (event_metrics, classification) in enumerate(events_data, 1):
            lines.append(f"{i}. {event_metrics.filename}")
            lines.append(f"   Case: {classification.case_str} - {classification.description}")
            lines.append(f"   Fault: {classification.fault}")
            lines.append(f"   Confidence: {classification.confidence:.1%}")
            lines.append("")
        return "\n".join(lines)


# -----------------------------------------------------------------------------
# CLI interface
# -----------------------------------------------------------------------------

def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="SRF Event Reporter - Generate comprehensive text analysis reports"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input directory containing parquet files or path to single parquet file")
    parser.add_argument("--output", type=str, required=True,
                        help="Output file path")
    parser.add_argument("--results", type=str, default="results",
                        help="Directory containing classification results (default: results)")
    parser.add_argument("--format", type=str, default="markdown",
                        choices=["markdown", "html", "plaintext", "json"],
                        help="Output format (default: markdown)")
    parser.add_argument("--sections", type=str,
                        help="Comma-separated list of sections to include (default: from config)")
    parser.add_argument("--batch", action="store_true",
                        help="Generate batch summary report for all events in input directory")
    parser.add_argument("--max-events", type=int, default=10,
                        help="Maximum events for batch report (default: 10)")
    parser.add_argument("--template", type=str,
                        help="Custom template file (overrides default)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    generator = ReportGenerator()

    sections = None
    if args.sections:
        sections = [s.strip() for s in args.sections.split(",")]

    input_path = Path(args.input)
    results_dir = Path(args.results)
    output_path = Path(args.output)

    if args.batch:
        if not input_path.is_dir():
            raise ValueError("Batch mode requires input directory")
        report = generator.generate_batch_report(
            parquet_dir=input_path, results_dir=results_dir,
            output_path=output_path, format=args.format, max_events=args.max_events,
        )
    else:
        if input_path.is_dir():
            parquet_files = list(input_path.glob("*.parquet"))
            if not parquet_files:
                raise ValueError(f"No parquet files found in {input_path}")
            parquet_path = parquet_files[0]
            print(f"Using first file: {parquet_path.name}")
        else:
            parquet_path = input_path

        report = generator.generate_report(
            parquet_path=parquet_path, results_dir=results_dir,
            output_path=output_path, format=args.format, sections=sections,
        )

    print(f"Report generated successfully: {output_path}")


if __name__ == "__main__":
    main()
