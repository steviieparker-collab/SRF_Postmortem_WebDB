"""
Unified configuration for SRF Postmortem.

Merges all sections from:
  - SRF_postmortem_WebDB: db, web, monitor
  - srf-event-monitoring-system: paths, email, classification, preprocessor,
    grouper, visualization, report, system, logging, monitoring
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import yaml
from pydantic import BaseModel, Field, field_validator


# ── Sub-models from monitoring system ─────────────────────────

class PathsConfig(BaseModel):
    """Configuration for file system paths."""

    watch_folders: List[Path] = Field(default_factory=list, description="Watch folders for scope CSV files")
    processed_dir: Path = Field(default=Path("./data/processed"), description="Preprocessor output directory")
    merged_dir: Path = Field(default=Path("./data/merged"), description="Grouper output directory")
    results_dir: Path = Field(default=Path("./data/results"), description="Classifier output directory")
    reports_dir: Path = Field(default=Path("./data/reports"), description="Report output directory")
    graphs_dir: Path = Field(default=Path("./data/graphs"), description="Graph images directory")
    log_dir: Path = Field(default=Path("./logs"), description="Log directory")
    log_file: str = Field(default="srf_monitor.log", description="Log file name")

    @field_validator("watch_folders", mode="before")
    @classmethod
    def convert_watch_folders(cls, v: Any) -> List[Path]:
        if isinstance(v, list):
            return [Path(str(item)) for item in v]
        return v

    @field_validator("processed_dir", "merged_dir", "results_dir", "reports_dir", "graphs_dir", "log_dir", mode="before")
    @classmethod
    def convert_path(cls, v: Any) -> Path:
        if isinstance(v, str):
            return Path(v)
        return v


class EmailConfig(BaseModel):
    smtp_server: str = Field(default="", description="SMTP server address")
    smtp_port: int = Field(default=587, description="SMTP port")
    sender_email: str = Field(default="", description="Sender email address")
    sender_password: str = Field(default="", description="Sender email password")
    receiver_emails: List[str] = Field(default_factory=list, description="List of receiver email addresses")
    group_wait_seconds: float = Field(default=30.0, description="Wait time for grouping events")
    max_attachments_per_email: int = Field(default=10, description="Maximum files per email")
    send_empty_events: bool = Field(default=False, description="Send emails even if no events")
    subject_template: str = Field(
        default="[Beamdump] {case} - {timestamp}",
        description="Email subject template"
    )
    body_template_file: Path = Field(default=Path("templates/email_body.md"), description="Path to email body template file")

    @field_validator("body_template_file", mode="before")
    @classmethod
    def convert_template_path(cls, v: Any) -> Path:
        if isinstance(v, str):
            return Path(v)
        return v

    @field_validator("receiver_emails", mode="before")
    @classmethod
    def split_receiver_emails(cls, v: Any) -> List[str]:
        if isinstance(v, str):
            return [email.strip() for email in v.split(",") if email.strip()]
        return v


class ClassificationConfig(BaseModel):
    """Configuration for event classification thresholds."""
    beam_min_v: float = Field(default=2.0, description="Minimum beam voltage (V)")
    dump_ratio: float = Field(default=0.5, description="Beam dump detection ratio")
    digital_min_persistence_ms: float = Field(default=0.01, description="0.01ms persistence for digital signals")
    digital_delay_compensation_ms: float = Field(default=0.4, description="Digital delay compensation")
    simultaneous_window_ms: float = Field(default=0.01, description="Events within 0.01ms are simultaneous")
    highhigh_threshold: float = Field(default=1.4, description="highhigh > 1.4")
    low_threshold: float = Field(default=0.9, description="0.5 < low < 0.9")
    lowlow_threshold: float = Field(default=0.5, description="lowlow < 0.5")
    baseline_start_s: float = Field(default=-0.5, description="Baseline start time (s)")
    baseline_end_s: float = Field(default=-0.45, description="Baseline end time (s)")
    baseline_clip_v: float = Field(default=0.01, description="Clip value to avoid division by zero")
    search_start_s: float = Field(default=-0.045, description="-45ms")
    search_end_s: float = Field(default=0.050, description="+50ms")


class PreprocessorConfig(BaseModel):
    segment_pre_s: float = Field(default=0.05, description="Pre-zero segment (s)")
    segment_post_s: float = Field(default=0.05, description="Post-zero segment (s)")
    decimation_factor: int = Field(default=2, description="Decimation factor")
    parquet_compression: str = Field(default="zstd", description="Compression algorithm")


class GrouperConfig(BaseModel):
    window_s: float = Field(default=180.0, description="Matching window (seconds)")
    wait_timeout: int = Field(default=300, description="Seconds to wait for all 3 scopes before failing (monitor mode)")
    merge_strategy: str = Field(default="full_join", description="full_join, inner_join, outer_join")


class DarkThemeConfig(BaseModel):
    bg_color: str = Field(default="#0f1117", description="Background color")
    panel_color: str = Field(default="#1a1d27", description="Panel color")
    grid_color: str = Field(default="#2a2d3a", description="Grid color")
    text_color: str = Field(default="#e0e0e0", description="Text color")
    title_color: str = Field(default="#ffffff", description="Title color")
    accent_color: str = Field(default="#4fc3f7", description="Accent color")


class TimeRange(BaseModel):
    name: str
    start_ms: float
    end_ms: float
    suffix: str


class VisualizationConfig(BaseModel):
    dark_theme: DarkThemeConfig = Field(default_factory=DarkThemeConfig)
    analog_colors: Dict[str, str] = Field(
        default={"B": "#FFE033", "C": "#00D4FF", "D": "#FF1E6B", "E": "#96D800"},
        description="Analog channel colors"
    )
    digital_palette: List[str] = Field(
        default_factory=lambda: [
            "#7986CB", "#4DB6AC", "#FFB74D", "#E57373",
            "#BA68C8", "#4DD0E1", "#AED581", "#F06292",
            "#64B5F6", "#A1887F", "#90A4AE", "#FFF176",
            "#80CBC4", "#CE93D8", "#FFCC02", "#80DEEA",
        ],
        description="Digital palette (16 channels)"
    )
    figure_size: List[int] = Field(default=[15, 8], description="Figure size [width, height]")
    dpi: int = Field(default=150, description="DPI for plots")
    time_ranges: List[TimeRange] = Field(
        default_factory=lambda: [
            TimeRange(name="wide", start_ms=-50, end_ms=50, suffix="_range1.jpg"),
            TimeRange(name="narrow", start_ms=-1, end_ms=1, suffix="_range2.jpg"),
        ],
        description="Time ranges for plotting"
    )


class ReportConfig(BaseModel):
    include_sections: List[str] = Field(
        default_factory=lambda: [
            "event_summary", "classification_result", "signal_metrics",
            "digital_events", "recommended_actions", "attachment_list",
        ],
        description="Sections to include in reports"
    )
    output_format: str = Field(default="markdown", description="markdown, html, plaintext")
    include_graphs: bool = Field(default=True, description="Include graphs in report")
    include_raw_data: bool = Field(default=False, description="Include raw data in report")
    max_table_rows: int = Field(default=10, description="Maximum rows in tables")
    truncate_long_values: bool = Field(default=True, description="Truncate long values")


class SystemConfig(BaseModel):
    mode: str = Field(default="monitor", description="monitor, batch, test")
    check_interval: float = Field(default=1.0, description="File check interval (seconds)")
    file_completion_timeout: int = Field(default=60, description="Wait for file completion (seconds)")
    max_file_age_days: int = Field(default=7, description="Delete files older than X days")
    max_workers: int = Field(default=4, description="Max concurrent processing threads")
    max_memory_mb: int = Field(default=1024, description="Memory usage limit")
    max_retries: int = Field(default=3, description="Maximum retry attempts")
    retry_delay_seconds: int = Field(default=5, description="Delay between retries")
    keep_intermediate_files: bool = Field(default=False, description="Clean up intermediate files")
    archive_processed_files: bool = Field(default=True, description="Archive processed CSV files")


class LoggingConfig(BaseModel):
    level: str = Field(default="INFO", description="DEBUG, INFO, WARNING, ERROR")
    format: str = Field(default="json", description="json, plain, structured")
    rotation: str = Field(default="daily", description="daily, weekly, 10MB")
    retention_days: int = Field(default=30, description="Log retention in days")
    console_enabled: bool = Field(default=True, description="Enable console output")
    console_format: str = Field(default="colored", description="Console format")
    file_enabled: bool = Field(default=True, description="Enable file output")
    max_file_size_mb: int = Field(default=10, description="Maximum log file size in MB")


class MonitoringConfig(BaseModel):
    system_tray_enabled: bool = Field(default=True, description="Enable system tray")
    tray_icon_color: str = Field(default="red", description="red, green, blue, yellow")
    notification_enabled: bool = Field(default=True, description="Enable notifications")
    notify_on_event: bool = Field(default=True, description="Notify on new events")
    notify_on_error: bool = Field(default=True, description="Notify on errors")
    notify_on_warning: bool = Field(default=False, description="Notify on warnings")
    status_update_interval: int = Field(default=60, description="Status update interval in seconds")


# ── Sub-models from WebDB ─────────────────────────────────────

class DBConfig(BaseModel):
    path: Path = Field(default=Path("./db/events.db"))

    @field_validator("path", mode="before")
    @classmethod
    def convert_db_path(cls, v: Any) -> Path:
        if isinstance(v, str):
            return Path(v)
        return v


class AccessConfig(BaseModel):
    password: str = ""
    session_ttl_minutes: int = 60


class WebConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 8050
    title: str = "SRF Postmortem Viewer"


class MonitorPathsConfig(BaseModel):
    data_root: str = ""
    merged_dir: str = "merged"
    graphs_dir: str = "graphs"
    reports_dir: str = "reports"


# ── Main unified configuration model ─────────────────────────

class AppConfig(BaseModel):
    """Unified application configuration with ALL sections from both projects."""

    # From monitoring system
    paths: PathsConfig = Field(default_factory=PathsConfig)
    email: EmailConfig = Field(default_factory=EmailConfig)
    classification: ClassificationConfig = Field(default_factory=ClassificationConfig)
    preprocessor: PreprocessorConfig = Field(default_factory=PreprocessorConfig)
    grouper: GrouperConfig = Field(default_factory=GrouperConfig)
    visualization: VisualizationConfig = Field(default_factory=VisualizationConfig)
    report: ReportConfig = Field(default_factory=ReportConfig)
    system: SystemConfig = Field(default_factory=SystemConfig)
    logging: LoggingConfig = Field(default_factory=LoggingConfig)
    monitoring: MonitoringConfig = Field(default_factory=MonitoringConfig)

    # Access control
    access: AccessConfig = Field(default_factory=AccessConfig)

    # From WebDB
    db: DBConfig = Field(default_factory=DBConfig)
    web: WebConfig = Field(default_factory=WebConfig)
    monitor: MonitorPathsConfig = Field(default_factory=MonitorPathsConfig)

    @classmethod
    def from_yaml(cls, config_path: Union[str, Path]) -> "AppConfig":
        config_path = Path(config_path)
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        with open(config_path, "r", encoding="utf-8") as f:
            yaml_data = yaml.safe_load(f)

        yaml_data = yaml_data or {}
        return cls(**yaml_data)


# ── Singleton management ──────────────────────────────────────

_config: Optional[AppConfig] = None


def load_config(config_path: Optional[Union[str, Path]] = None) -> AppConfig:
    if config_path is None:
        config_path = Path("./config/config.yaml")

    config_path = Path(config_path)

    if not config_path.exists():
        # Return default config if no config file exists
        return AppConfig()

    return AppConfig.from_yaml(config_path)


def get_config() -> AppConfig:
    global _config
    if _config is None:
        _config = load_config()
    return _config


def get_settings() -> AppConfig:
    """Alias for get_config() — backward compatibility with monitoring code."""
    return get_config()


def reset_config() -> None:
    global _config
    _config = None
