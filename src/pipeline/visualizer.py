"""
Visualizer module for SRF Event Monitoring System.

Generates dark theme waveform plots for SRF events in configurable time ranges.
Supports both raw parquet files and classification results overlay.
"""

import logging
import os
import sys
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Union
import pandas as pd
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import datetime

from ..core.config import get_config

# Setup basic logging if not configured
logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False


class Visualizer:
    """
    Visualizer for SRF event waveforms.

    Uses configuration-driven styling, supports dark theme, multiple time ranges,
    and classification result overlay.
    """

    def __init__(self, config=None):
        """
        Initialize visualizer with configuration.

        Args:
            config: Optional configuration. Can be:
                   - None: Load from settings
                   - AppConfig: Use config.visualization
                   - VisualizationConfig: Use directly
                   - dict: Treated as visualization config
        """
        from ..core.config import AppConfig, VisualizationConfig

        if config is None:
            settings = get_config()
            self.config = settings.visualization
        elif isinstance(config, AppConfig):
            self.config = config.visualization
        elif isinstance(config, VisualizationConfig):
            self.config = config
        elif isinstance(config, dict):
            self.config = VisualizationConfig(**config)
        else:
            self.config = config

        dark_theme = self.config.dark_theme
        if isinstance(dark_theme, dict):
            class ThemeObject:
                def __init__(self, d):
                    self.__dict__.update(d)
            self.theme = ThemeObject(dark_theme)
        elif hasattr(dark_theme, 'dict'):
            self.theme = dark_theme
        else:
            self.theme = dark_theme
        self.analog_colors = self.config.analog_colors
        self.digital_palette = self.config.digital_palette
        self.figure_size = tuple(self.config.figure_size)
        self.dpi = self.config.dpi
        time_ranges = self.config.time_ranges
        if time_ranges and hasattr(time_ranges[0], 'dict'):
            self.time_ranges = [
                {
                    "name": r.name,
                    "start_ms": r.start_ms,
                    "end_ms": r.end_ms,
                    "suffix": r.suffix,
                }
                for r in time_ranges
            ]
        else:
            self.time_ranges = time_ranges

        self._setup_rcparams()

    def _setup_rcparams(self):
        """Configure matplotlib rcParams based on theme."""
        plt.rcParams.update({
            'font.family': 'DejaVu Sans',
            'font.size': 10,
            'axes.titlesize': 12,
            'axes.labelsize': 11,
            'xtick.labelsize': 10,
            'ytick.labelsize': 10,
            'figure.facecolor': self.theme.bg_color,
            'axes.facecolor': self.theme.panel_color,
            'axes.edgecolor': self.theme.grid_color,
            'axes.labelcolor': self.theme.text_color,
            'xtick.color': self.theme.text_color,
            'ytick.color': self.theme.text_color,
            'text.color': self.theme.text_color,
            'grid.color': self.theme.grid_color,
            'grid.linestyle': '--',
            'grid.linewidth': 0.6,
            'grid.alpha': 0.8,
            'legend.facecolor': '#1e2130',
            'legend.edgecolor': '#3a3d50',
            'legend.framealpha': 0.9,
            'legend.fontsize': 9,
        })

    def load_data(self, filepath: Union[str, Path]) -> pd.DataFrame:
        """Load waveform data from CSV or Parquet file."""
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Data file not found: {filepath}")

        if filepath.suffix.lower() == '.csv':
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            labels = lines[19].strip().split(',')
            df = pd.read_csv(pd.io.common.StringIO(''.join(lines[21:])), header=None)
            if len(labels) > 6:
                labels.pop(6)
            if df.shape[1] > 6:
                df.drop(columns=[6], inplace=True)
            df.columns = labels
            logger.info(f"Loaded CSV data with shape {df.shape}")
        elif filepath.suffix.lower() in ['.parquet', '.pq']:
            import polars as pl
            df_pl = pl.read_parquet(filepath)
            if "event_timestamp" in df_pl.columns:
                df_pl = df_pl.drop("event_timestamp")
            df = df_pl.to_pandas()
            logger.info(f"Loaded Parquet data with shape {df.shape}")

            time_col = None
            possible_time = ['Time', 'time', 't', 'T']
            for col in possible_time:
                if col in df.columns:
                    time_col = col
                    break
            if time_col is None and len(df.columns) > 0:
                time_col = df.columns[0]

            if time_col and time_col in df.columns:
                if pd.api.types.is_datetime64_any_dtype(df[time_col]):
                    logger.warning(f"Time column '{time_col}' is datetime, converting to float seconds")
                    df[time_col] = df[time_col].astype(float)
                elif not pd.api.types.is_numeric_dtype(df[time_col]):
                    df[time_col] = pd.to_numeric(df[time_col], errors='coerce')
                    logger.info(f"Converted time column '{time_col}' to float")

            for col in df.columns:
                if col == time_col:
                    continue
                if not pd.api.types.is_numeric_dtype(df[col]):
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    logger.debug(f"Converted column '{col}' to float")
        else:
            raise ValueError(f"Unsupported file format: {filepath.suffix}")

        return df

    def infer_channel_columns(self, df: pd.DataFrame) -> Tuple[str, List[str], List[str]]:
        """Infer time column, analog channels, and digital channels."""
        import re
        possible_time = ['Time', 'time', 't', 'T', 't_rel_s', 'timestamp', 'event_timestamp']
        time_col = None
        for col in possible_time:
            if col in df.columns:
                time_col = col
                break
        if time_col is None:
            time_col = df.columns[0]

        stat_patterns = [
            '_baseline_std', '_mean', '_std', '_min', '_max', '_median',
            '_baseline_mean', '_baseline_stddev', '_baseline_min', '_baseline_max',
            'Time_v', 'time_v', 'timestamp_v'
        ]

        digital_cols = []
        analog_cols = []

        for col in df.columns:
            if col == time_col:
                continue
            if col.endswith('_timestamp') or col.endswith('_ts'):
                continue
            if any(pattern in col for pattern in stat_patterns):
                logger.debug(f"Skipping statistical column: {col}")
                continue
            if col.endswith('_d'):
                digital_cols.append(col)
            elif col.endswith('_v'):
                analog_cols.append(col)
            elif col in ['B', 'C', 'D', 'E']:
                analog_cols.append(col)
            elif re.match(r'^CH\d+_v$', col):
                analog_cols.append(col)
            else:
                logger.debug(f"Unknown column type, skipping: {col}")

        if not digital_cols:
            for col in df.columns:
                if col == time_col:
                    continue
                if re.match(r'^D\d+_d$', col):
                    digital_cols.append(col)
                elif col.startswith('D') and col[1:].isdigit():
                    digital_cols.append(col)

        if not digital_cols and len(df.columns) >= 22:
            digital_cols = list(df.columns[6:22])
            digital_cols = [col for col in digital_cols
                           if not any(pattern in col for pattern in stat_patterns)]
            analog_cols = [col for col in df.columns
                          if col not in digital_cols and col != time_col
                          and not any(pattern in col for pattern in stat_patterns)]

        logger.debug(f"Detected time column: {time_col}")
        logger.debug(f"Detected analog columns ({len(analog_cols)}): {analog_cols}")
        logger.debug(f"Detected digital columns ({len(digital_cols)}): {digital_cols}")
        return time_col, analog_cols, digital_cols

    def plot_single(
        self,
        input_path: Union[str, Path],
        output_path: Union[str, Path],
        time_range: str = "wide",
        classification: Optional[Dict[str, Any]] = None,
        event_markers: Optional[List[Dict[str, Any]]] = None,
        style: str = "default",
    ) -> bool:
        """Generate a single plot from input file."""
        try:
            range_config = next((tr for tr in self.time_ranges if tr['name'] == time_range), None)
            if range_config is None:
                logger.error(f"Unknown time range: {time_range}")
                return False

            start_s = range_config['start_ms'] / 1000.0
            end_s = range_config['end_ms'] / 1000.0

            df = self.load_data(input_path)

            if style == "event_labeller":
                success = self.plot_event_labeller_style(
                    df=df,
                    output_path=output_path,
                    time_range=(start_s, end_s),
                    classification=classification,
                    event_markers=event_markers,
                    title_suffix=time_range.capitalize(),
                )
                if success:
                    logger.info(f"Event-labeller plot saved to {output_path}")
                return success
            else:
                self.plot_waveform(
                    df=df,
                    output_path=output_path,
                    time_range=(start_s, end_s),
                    classification=classification,
                    event_markers=event_markers,
                    title_suffix=time_range.capitalize(),
                )
                logger.info(f"Plot saved to {output_path}")
                return True

        except Exception as e:
            logger.error(f"Failed to generate plot: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def plot_waveform(
        self,
        df: pd.DataFrame,
        output_path: Union[str, Path],
        time_range: Tuple[float, float],
        classification: Optional[Dict[str, Any]] = None,
        event_markers: Optional[List[Dict[str, Any]]] = None,
        title_suffix: str = "",
    ) -> None:
        """Generate a combined waveform plot with dark theme styling."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        time_col, analog_cols, digital_cols = self.infer_channel_columns(df)

        start_s, end_s = time_range
        df_range = df[(df[time_col] >= start_s) & (df[time_col] <= end_s)].copy()
        if df_range.empty:
            logger.warning(f"No data in time range {start_s} to {end_s}. Skipping plot.")
            return

        fig, ax = plt.subplots(figsize=self.figure_size)
        fig.patch.set_facecolor(self.theme.bg_color)

        self._plot_analog(ax, df_range, time_col, analog_cols)
        if digital_cols:
            self._plot_digital(ax, df_range, time_col, digital_cols)

        if classification:
            self._add_classification_overlay(ax, classification)
        if event_markers:
            self._add_event_markers(ax, event_markers, time_col)

        self._decorate_plot(ax, df_range, time_col, analog_cols, digital_cols, output_path, start_s, end_s, title_suffix)

        plt.tight_layout(pad=1.8)
        fig.subplots_adjust(right=0.82)
        plt.savefig(output_path, dpi=self.dpi, facecolor=self.theme.bg_color)
        plt.close()

        logger.info(f"Saved plot to {output_path}")

    def _plot_analog(self, ax, df, time_col, analog_cols):
        """Plot analog channels with scaling and initial markers."""
        scaling_map = {
            'B': lambda v: (v / 2.0) * 7,
            'C': lambda v: v * 35,
            'D': lambda v: v * 35,
            'E': lambda v: v * 35,
        }
        time_vals = df[time_col] * 1000
        x_left = df[time_col].iloc[0] * 1000

        for col in analog_cols:
            key = col
            if key not in self.analog_colors:
                idx = analog_cols.index(col) % len(self.digital_palette)
                color = self.digital_palette[idx]
            else:
                color = self.analog_colors[key]

            if key.startswith('CH'):
                scale_fn = lambda v: v * 35
            else:
                scale_fn = scaling_map.get(key, lambda v: v)

            scaled = scale_fn(df[col])

            ax.plot(time_vals, scaled, color=color, linewidth=2.2,
                    alpha=0.85, label=col, solid_capstyle='round')
            ax.plot(time_vals, scaled, color=color, linewidth=0.7,
                    alpha=1.0, label='_nolegend_')

            init_val = scaled.iloc[0]
            try:
                init_y = float(init_val)
            except (TypeError, ValueError):
                if hasattr(init_val, 'timestamp'):
                    init_y = init_val.timestamp()
                else:
                    init_y = 0.0
                    logger.warning(f"Could not convert initial value to float: {init_val}")

            ax.plot(x_left, init_y,
                    marker='<', markersize=11,
                    markerfacecolor=color, markeredgecolor='white',
                    markeredgewidth=1.2,
                    zorder=6, clip_on=False, label='_nolegend_')
            try:
                val_str = f'{df[col].iloc[0]:.3f}'
            except (TypeError, ValueError):
                val_str = str(df[col].iloc[0])

            ax.annotate(val_str,
                        xy=(x_left, init_y),
                        xytext=(6, 0), textcoords='offset points',
                        fontsize=7.5, color=color, alpha=0.85,
                        va='center')

    def _plot_digital(self, ax, df, time_col, digital_cols):
        """Plot digital channels as step plots with channel labels."""
        gap = 1.2
        start_y = 38
        time_vals = df[time_col] * 1000
        label_x = time_vals.iloc[0]

        for i, col in enumerate(digital_cols):
            y = df[col].fillna(0)
            y_base = start_y - i * gap
            color = self.digital_palette[i % len(self.digital_palette)]

            ax.axhspan(y_base - 0.05, y_base + 1.1,
                       alpha=0.04, color=color, zorder=0)
            ax.step(time_vals, y + y_base, where='post',
                    color=color, linewidth=1.1,
                    alpha=0.9, label='_nolegend_')
            ax.text(label_x, y_base + 0.25, col,
                    fontsize=8, color=color,
                    ha='left', va='bottom',
                    bbox=dict(boxstyle='round,pad=0.15',
                              facecolor=self.theme.panel_color,
                              edgecolor=color,
                              alpha=0.7,
                              linewidth=0.6))

    def _add_classification_overlay(self, ax, classification):
        """Add classification result as watermark or annotation."""
        case = classification.get('case', 'N/A')
        fault_desc = classification.get('fault_description', '')
        groups = classification.get('groups', [])

        lines = [f"Case {case}"]
        if fault_desc:
            lines.append(fault_desc)
        if groups:
            lines.append(f"Groups: {', '.join(groups)}")

        text = '\n'.join(lines)
        ax.text(0.98, 0.98, text,
                transform=ax.transAxes,
                fontsize=9,
                color=self.theme.accent_color,
                alpha=0.8,
                ha='right', va='top',
                bbox=dict(boxstyle='round,pad=0.3',
                          facecolor=self.theme.bg_color,
                          edgecolor=self.theme.accent_color,
                          alpha=0.7))

    def _add_event_markers(self, ax, event_markers, time_col):
        """Add vertical lines for event markers."""
        for marker in event_markers:
            time_s = marker['time']
            label = marker.get('label', '')
            color = marker.get('color', self.theme.accent_color)
            style = marker.get('style', '--')

            ax.axvline(x=time_s * 1000, color=color, linestyle=style, linewidth=1, alpha=0.7)
            ax.text(time_s * 1000, ax.get_ylim()[1], label,
                    fontsize=8, color=color, ha='center', va='bottom',
                    rotation=90, alpha=0.8)

    def _decorate_plot(self, ax, df, time_col, analog_cols, digital_cols, output_path, start_s, end_s, title_suffix):
        """Add axis labels, title, grid, and legend."""
        ax.set_xlabel("Time (ms)", fontsize=11,
                      color=self.theme.text_color, labelpad=6)
        ax.set_ylabel("Amplitude (a.u.)", fontsize=11,
                      color=self.theme.text_color, labelpad=6)

        folder_name = output_path.parent.name
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_range_str = f"{start_s * 1000:.1f} ms  ~  {end_s * 1000:+.1f} ms"
        title = f"SRF Postmortem Waveform  |  {folder_name}\n{time_range_str}    {now_str}"
        if title_suffix:
            title += f"\n{title_suffix}"
        ax.set_title(title, fontsize=12,
                     color=self.theme.title_color,
                     fontweight='bold', pad=12, linespacing=1.6)

        ax.grid(True, which='major', zorder=1)

        time_vals = df[time_col] * 1000
        ax.set_xlim(time_vals.iloc[0], time_vals.iloc[-1])

        if analog_cols:
            y_min = df[analog_cols].min().min()
            y_max_analog = df[analog_cols].max().max()
        else:
            y_min = 0
            y_max_analog = 0

        start_y = 38
        gap = 1.2
        digital_top = start_y + 2
        if digital_cols:
            digital_bottom = start_y - (len(digital_cols) - 1) * gap - 1
        else:
            digital_bottom = start_y

        y_lower = min(y_min - 1, digital_bottom)
        y_upper = max(41, y_max_analog + 5, digital_top)
        ax.set_ylim(y_lower, y_upper)

        for spine in ax.spines.values():
            spine.set_edgecolor(self.theme.grid_color)
            spine.set_linewidth(0.8)

        handles, labels = ax.get_legend_handles_labels()
        if handles:
            leg = ax.legend(
                loc='upper left',
                bbox_to_anchor=(1.0, 1),
                borderaxespad=0,
                frameon=True,
                handlelength=2.0,
                handleheight=1.2,
                labelspacing=0.6,
            )
            for text in leg.get_texts():
                text.set_color(self.theme.text_color)

    def generate_plots(
        self,
        input_path: Union[str, Path],
        output_dir: Optional[Union[str, Path]] = None,
        classification: Optional[Dict[str, Any]] = None,
        event_markers: Optional[List[Dict[str, Any]]] = None,
        ranges: Optional[List[str]] = None,
    ) -> List[Path]:
        """Generate plots for all configured time ranges."""
        input_path = Path(input_path)
        if output_dir is None:
            output_dir = input_path.parent
        output_dir = Path(output_dir)
        output_dir.mkdir(parents=True, exist_ok=True)

        df = self.load_data(input_path)

        if ranges is None:
            ranges = [tr['name'] for tr in self.time_ranges]
        range_configs = [tr for tr in self.time_ranges if tr['name'] in ranges]

        saved_paths = []
        for tr in range_configs:
            start_ms = tr['start_ms']
            end_ms = tr['end_ms']
            suffix = tr.get('suffix', f"_{tr['name']}.jpg")
            output_path = output_dir / f"{input_path.stem}{suffix}"

            self.plot_waveform(
                df=df,
                output_path=output_path,
                time_range=(start_ms / 1000.0, end_ms / 1000.0),
                classification=classification,
                event_markers=event_markers,
                title_suffix=tr['name'].capitalize(),
            )
            saved_paths.append(output_path)

        return saved_paths

    def plot_event_labeller_style(
        self,
        df: pd.DataFrame,
        output_path: Union[str, Path],
        time_range: Tuple[float, float],
        classification: Optional[Dict[str, Any]] = None,
        event_markers: Optional[List[Dict[str, Any]]] = None,
        title_suffix: str = "",
    ) -> bool:
        """Generate event-labeller style plot with two subplots."""
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        time_col, analog_cols, digital_cols = self.infer_channel_columns(df)

        start_s, end_s = time_range
        df_range = df[(df[time_col] >= start_s) & (df[time_col] <= end_s)].copy()
        if df_range.empty:
            logger.warning(f"No data in time range {start_s} to {end_s}. Skipping plot.")
            return False

        fig, (ax1, ax2) = plt.subplots(
            2, 1,
            figsize=(12, 8),
            sharex=True,
            gridspec_kw={'height_ratios': [4, 3]}
        )
        fig.patch.set_facecolor(self.theme.bg_color)
        time_vals = df_range[time_col] * 1000

        # Analog plot (top)
        analog_colors = [
            "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
            "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        ]

        for i, col in enumerate(analog_cols):
            color = analog_colors[i % len(analog_colors)]
            signal_values = df_range[col]
            ax1.plot(time_vals, signal_values, label=col, linewidth=1.5, alpha=0.8, color=color)

        if analog_cols:
            ax1.legend(loc="upper right", fontsize=7, ncol=3, frameon=True)
            ax1.set_ylabel("Analog Signal (Original)", fontsize=9, color=self.theme.text_color)

        ax1.grid(True, linestyle="--", alpha=0.4, color=self.theme.grid_color)
        ax1.set_facecolor(self.theme.panel_color)

        # Digital plot (bottom)
        digital_palette = self.digital_palette
        num_dig = len(digital_cols)

        digital_spacing = 10
        digital_amplitude = 9.0

        for i, col in enumerate(digital_cols):
            offset = (num_dig - 1 - i) * digital_spacing
            color = digital_palette[i % len(digital_palette)]
            digital_values = df_range[col] * digital_amplitude
            ax2.step(time_vals, digital_values + offset,
                    where="post", label=col, linewidth=1.2, color=color, alpha=0.9)

        if digital_cols:
            y_min = -0.5
            y_max = num_dig * digital_spacing + digital_amplitude
            ax2.set_ylim(y_min, y_max)
            ax2.legend(loc="center left", bbox_to_anchor=(1.02, 0.5),
                      fontsize=6.5, ncol=1, frameon=False, labelspacing=0.05)
            ax2.set_ylabel("Digital Channels", fontsize=9, color=self.theme.text_color)

        ax2.set_xlabel("Time (ms)", fontsize=9, color=self.theme.text_color)
        ax2.grid(True, linestyle="--", alpha=0.4, color=self.theme.grid_color)
        ax2.set_facecolor(self.theme.panel_color)

        # Title
        folder_name = output_path.parent.name
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        time_range_str = f"{start_s * 1000:.1f} ms  ~  {end_s * 1000:+.1f} ms"
        title = f"SRF Event Analysis  |  {folder_name}\n{time_range_str}    {now_str}"
        if title_suffix:
            title += f"\n{title_suffix}"
        fig.suptitle(title, fontsize=12,
                     color=self.theme.title_color,
                     fontweight='bold', y=0.98)

        plt.tight_layout(rect=[0, 0, 0.95, 0.96])
        plt.savefig(output_path, dpi=self.dpi, facecolor=self.theme.bg_color)
        plt.close()

        logger.info(f"Saved event-labeller style plot to {output_path}")
        return True


def main():
    """Command-line interface for visualizer."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Generate dark theme waveform plots for SRF events."
    )
    parser.add_argument("--input", "-i", required=True, help="Input data file (CSV or Parquet).")
    parser.add_argument("--output", "-o", help="Output image file path.")
    parser.add_argument("--range", "-r", choices=['wide', 'narrow'], default='wide',
                        help="Time range to plot: wide (-50ms to +50ms) or narrow (-1ms to +1ms).")
    parser.add_argument("--output-dir", "-d", help="Output directory for batch processing.")
    parser.add_argument("--input-dir", help="Process all CSV/Parquet files in directory (batch mode).")
    parser.add_argument("--classification-json", help="Path to classification result JSON file.")
    parser.add_argument("--verbose", "-v", action="store_true", help="Enable verbose logging.")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(level=log_level, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

    viz = Visualizer()

    classification = None
    if args.classification_json:
        import json
        with open(args.classification_json, 'r') as f:
            classification = json.load(f)

    if args.input_dir:
        input_dir = Path(args.input_dir)
        if not input_dir.exists():
            logger.error(f"Input directory not found: {input_dir}")
            sys.exit(1)
        output_dir = Path(args.output_dir) if args.output_dir else input_dir / 'graphs'
        output_dir.mkdir(parents=True, exist_ok=True)

        extensions = ('.csv', '.parquet', '.pq')
        input_files = []
        for ext in extensions:
            input_files.extend(input_dir.glob(f"*{ext}"))

        for input_file in input_files:
            logger.info(f"Processing {input_file.name}")
            try:
                viz.generate_plots(
                    input_path=input_file,
                    output_dir=output_dir,
                    classification=classification,
                    ranges=[args.range] if args.range else None,
                )
            except Exception as e:
                logger.error(f"Failed to process {input_file}: {e}")
    else:
        input_path = Path(args.input)
        if not input_path.exists():
            logger.error(f"Input file not found: {input_path}")
            sys.exit(1)

        if args.output:
            output_path = Path(args.output)
            range_config = next((tr for tr in viz.time_ranges if tr['name'] == args.range), None)
            if range_config is None:
                logger.error(f"Unknown range: {args.range}")
                sys.exit(1)
            start_s = range_config['start_ms'] / 1000.0
            end_s = range_config['end_ms'] / 1000.0

            df = viz.load_data(input_path)
            viz.plot_waveform(
                df=df,
                output_path=output_path,
                time_range=(start_s, end_s),
                classification=classification,
            )
        else:
            output_dir = Path(args.output_dir) if args.output_dir else input_path.parent
            viz.generate_plots(
                input_path=input_path,
                output_dir=output_dir,
                classification=classification,
                ranges=[args.range] if args.range else None,
            )

    logger.info("Visualization complete.")


if __name__ == "__main__":
    main()
