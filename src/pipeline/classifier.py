"""
SRF Event Classifier Module (Monitoring Pipeline).

Rule-based classification of SRF events into 13 cases based on expert logic diagram.
Uses configuration for thresholds, integrates with core modules (config, logger, utils).

NOTE: This is the PIPELINE classifier (AcceleratorEventClassifier) — separate from
the WebDB classifier in src/classifier/classifier.py. This one runs on parquet files
and produces classification JSON + overlay data for visualizer/reporter/email.
"""

from __future__ import annotations

import argparse
import json
import logging
import warnings
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional, Dict, List, Tuple, Any, Union
from datetime import datetime

import numpy as np
import pandas as pd

# Core modules
from ..core.config import get_config
from ..core.logger import get_logger
from ..core.utils import classify_columns, safe_read_parquet

# Pipeline datatypes (RuleEngine, EventDetector, EventGrouper defined in this file)
from ..pipeline.datatypes import SignalEvent, TimeGroup

warnings.filterwarnings("ignore")

# -----------------------------------------------------------------------------
# Constants from configuration
# -----------------------------------------------------------------------------

def get_classification_config():
    """Return classification configuration."""
    return get_config().classification


# Time column name (should be consistent across data)
TIME_COL = "t_rel_s"

# -----------------------------------------------------------------------------
# Event detection
# -----------------------------------------------------------------------------


class EventDetector:
    """Detects analog and digital events with configurable thresholds."""

    def __init__(self, config=None):
        self.config = config or get_classification_config()
        self.logger = get_logger(__name__)

    def detect_analog_events(self, df: pd.DataFrame, channel: str, t: np.ndarray) -> List[SignalEvent]:
        """
        Detect low, lowlow, highhigh events in analog channel.
        REFLECT channels are excluded (per logic diagram).
        """
        if channel.startswith("Reflect"):
            return []

        events = []
        vals = df[channel].to_numpy(dtype=float)

        mask = (t >= self.config.search_start_s) & (t <= self.config.search_end_s)
        if not np.any(mask):
            return events

        window_vals = vals[mask]
        window_t = t[mask]

        # Find lowlow (< lowlow_threshold)
        lowlow_mask = window_vals < self.config.lowlow_threshold
        if np.any(lowlow_mask):
            first_idx = np.argmax(lowlow_mask)
            events.append(SignalEvent(
                time=float(window_t[first_idx]),
                channel=channel,
                event_type="lowlow",
                value=float(window_vals[first_idx])
            ))

        # Find low (lowlow_threshold < low < low_threshold)
        low_mask = (window_vals > self.config.lowlow_threshold) & (window_vals < self.config.low_threshold)
        if np.any(low_mask):
            first_idx = np.argmax(low_mask)
            events.append(SignalEvent(
                time=float(window_t[first_idx]),
                channel=channel,
                event_type="low",
                value=float(window_vals[first_idx])
            ))

        # Find highhigh (> highhigh_threshold)
        highhigh_mask = window_vals > self.config.highhigh_threshold
        if np.any(highhigh_mask):
            first_idx = np.argmax(highhigh_mask)
            events.append(SignalEvent(
                time=float(window_t[first_idx]),
                channel=channel,
                event_type="highhigh",
                value=float(window_vals[first_idx])
            ))

        return events

    def detect_digital_events(self, df: pd.DataFrame, channel: str, t: np.ndarray) -> List[SignalEvent]:
        """
        Detect digital signal changes (0->1 and 1->0 transitions) with noise filtering.
        """
        events = []
        if channel not in df.columns:
            return events

        vals = df[channel].to_numpy(dtype=float)

        mask = (t >= self.config.search_start_s) & (t <= self.config.search_end_s)
        if not np.any(mask):
            return events

        window_vals = vals[mask]
        window_t = t[mask]

        if len(window_t) < 2:
            return events

        dt = np.mean(np.diff(window_t))

        min_persistence_s = self.config.digital_min_persistence_ms / 1000.0
        min_samples_needed = max(1, int(np.ceil(min_persistence_s / dt - 0.0005)))

        diff = np.diff(window_vals)
        change_indices = np.where(diff != 0)[0]

        if len(change_indices) == 0:
            return events

        change_indices = list(change_indices) + [len(window_vals) - 1]

        for i in range(len(change_indices) - 1):
            change_idx = change_indices[i]
            next_change_idx = change_indices[i + 1]

            new_value = window_vals[change_idx + 1]
            change_time = window_t[change_idx + 1]

            if next_change_idx == len(window_vals) - 1:
                consecutive_samples = len(window_vals) - (change_idx + 1)
            else:
                consecutive_samples = next_change_idx - change_idx

            if consecutive_samples >= min_samples_needed:
                import re
                is_int_fc = re.match(r'INT_(IC|IN)_FC\d+_d$', channel)
                if is_int_fc and new_value == 0.0:
                    continue

                is_int_mis = re.match(r'INT_MIS(1|2)_IC_d$', channel)
                if is_int_mis and new_value == 0.0:
                    continue

                is_cm_channel = re.match(r'(RDY|QUEN|VAC|ARC|HE-PR)_CM\d+_FC\d+_d$', channel)
                if is_cm_channel and new_value == 0.0:
                    continue

                events.append(SignalEvent(
                    time=float(change_time),
                    channel=channel,
                    event_type="digital",
                    value=new_value
                ))

        return events


# -----------------------------------------------------------------------------
# Time grouping
# -----------------------------------------------------------------------------


class EventGrouper:
    """Groups events into FIRST, SECOND, THIRD based on effective time."""

    def __init__(self, config=None):
        self.config = config or get_classification_config()
        self.logger = get_logger(__name__)

    def group_events(self, events: List[SignalEvent]) -> Tuple[TimeGroup, TimeGroup, TimeGroup]:
        """
        Group events into FIRST, SECOND, THIRD based on new logic:
        1. Most early event is FIRST
        2. Events within simultaneous_window_ms of the first event are also FIRST
        3. THIRD is BeamCurrent_lowlow (always exists, scope trigger)
        4. Events between FIRST and THIRD are SECOND
        """
        if not events:
            return TimeGroup("FIRST"), TimeGroup("SECOND"), TimeGroup("THIRD")

        sorted_events = sorted(events, key=lambda e: e.effective_time)

        third_event = None
        for event in sorted_events:
            if event.channel == "BeamCurrent_v" and event.event_type == "lowlow":
                third_event = event
                break

        first_group = TimeGroup("FIRST")
        second_group = TimeGroup("SECOND")
        third_group = TimeGroup("THIRD")

        if third_event:
            third_group.add_event(third_event)
            third_group.start_time = third_event.effective_time

        events_before_third = []
        events_after_third = []

        for event in sorted_events:
            if event is third_event:
                continue
            if third_event and event.effective_time < third_event.effective_time:
                events_before_third.append(event)
            else:
                events_after_third.append(event)

        if not third_event:
            events_before_third = sorted_events

        if events_before_third:
            first_event = events_before_third[0]
            first_group.start_time = first_event.effective_time
            first_group.add_event(first_event)

            for event in events_before_third[1:]:
                time_diff = abs(event.effective_time - first_group.start_time) * 1000
                if time_diff <= self.config.simultaneous_window_ms:
                    first_group.add_event(event)
                else:
                    if second_group.start_time == 0.0:
                        second_group.start_time = event.effective_time
                    second_group.add_event(event)

        for event in events_after_third:
            if second_group.start_time == 0.0:
                second_group.start_time = event.effective_time
            second_group.add_event(event)

        return first_group, second_group, third_group


# -----------------------------------------------------------------------------
# Internal RuleEngine (for the monitoring pipeline classifier)
# -----------------------------------------------------------------------------

class _InternalRuleEngine:
    """Rule engine used internally by AcceleratorEventClassifier."""

    def __init__(self):
        self.logger = get_logger(__name__)

    def apply_rules(self, first: TimeGroup, second: TimeGroup, third: TimeGroup,
                   all_events: List[SignalEvent]) -> Dict[str, Any]:
        """Apply rules in order, return first matching case."""
        import re

        def has_in_group(group: TimeGroup, channel: str, event_type: str = None) -> bool:
            for e in group.events:
                if e.channel == channel:
                    if event_type is None or e.event_type == event_type:
                        if event_type == "digital" and e.value != 1.0:
                            continue
                        return True
            return False

        def get_channels_with_event(group: TimeGroup, channel_prefix: str, event_type: str = None) -> List[str]:
            channels = []
            for e in group.events:
                if e.channel.startswith(channel_prefix):
                    if event_type is None or e.event_type == event_type:
                        if event_type == "digital" and e.value != 1.0:
                            continue
                        channels.append(e.channel)
            return channels

        def format_channel_names(channels: List[str]) -> str:
            if not channels:
                return ""
            formatted = []
            for ch in channels:
                clean = ch.replace("_v", "")
                formatted.append(clean)
            return ", ".join(formatted)

        def extract_srf_number(channel_name: str) -> str:
            match = re.search(r'SRF(\d+)', channel_name)
            if match:
                return match.group(1)
            parts = channel_name.split('_')
            for part in parts:
                if part.isdigit():
                    return part
            return "#"

        def format_fault_with_numbers(fault_template: str, channels: List[str]) -> str:
            if not channels:
                return fault_template
            numbers = []
            for ch in channels:
                num = extract_srf_number(ch)
                if num != "#":
                    numbers.append(num)
            if not numbers:
                return fault_template
            numbers_str = ",".join(sorted(set(numbers)))
            fault_msg = fault_template.replace("#", numbers_str)
            if len(numbers) > 1 and "station#" in fault_msg:
                fault_msg = fault_msg.replace("station" + numbers_str, "stations " + numbers_str)
            return fault_msg

        # Rule 1: BeamCurrent low in FIRST
        if has_in_group(first, "BeamCurrent_v", "low"):
            return {"case": 1, "description": "BeamCurrent is low",
                    "fault": "Beam loss, check other systems like MPS, feedback system", "confidence": 0.95}

        # Rule 2: BeamCurrent lowlow in FIRST
        if has_in_group(first, "BeamCurrent_v", "lowlow"):
            return {"case": 2, "description": "BeamCurrent is lowlow",
                    "fault": "Beam loss, check other systems like MPS, feedback system", "confidence": 0.95}

        # Rule 3: Any single digital in FIRST (excluding INT_MIS/INT_PSI)
        digital_in_first = [e for e in first.events if e.event_type == "digital" and e.value == 1.0]
        if len(digital_in_first) == 1:
            dc = digital_in_first[0].channel
            if not re.match(r'INT_MIS(1|2)_IC_d$', dc) and not re.match(r'INT_PSI(1|2)_IC_d$', dc):
                sn = dc.replace("_IC_d", "").replace("_d", "")
                return {"case": 3, "description": f"Single digital: {dc}",
                        "fault": f"\"{sn}\" is the fault", "confidence": 0.90}

        # Rule 4: INT_MIS in FIRST
        mis_channels = [e.channel for e in first.events if e.event_type == "digital" and e.value == 1.0
                       and re.match(r'INT_MIS(1|2)_IC_d$', e.channel)]
        if mis_channels:
            other_digital = any(
                e.event_type == "digital" and e.value == 1.0
                and not re.match(r'INT_MIS(1|2)_IC_d$', e.channel)
                for e in first.events
            )
            if not other_digital:
                return {"case": 4, "description": ", ".join(sorted(c.replace("_d", "") for c in mis_channels)),
                        "fault": "INT_MIS_IC is the fault.", "confidence": 0.85}

        # Rule 5: INT_PSI in FIRST
        psi_channels = [e.channel for e in first.events if e.event_type == "digital" and e.value == 1.0
                       and re.match(r'INT_PSI(1|2)_IC_d$', e.channel)]
        if psi_channels:
            other_digital = any(
                e.event_type == "digital" and e.value == 1.0
                and not re.match(r'INT_PSI(1|2)_IC_d$', e.channel)
                for e in first.events
            )
            if not other_digital:
                return {"case": 5, "description": ", ".join(sorted(c.replace("_d", "") for c in psi_channels)),
                        "fault": "INT_PSI_IC is the fault.", "confidence": 0.85}

        # Rule 6: Multi-digital same group
        rdy_channels = [e.channel for e in first.events if e.event_type == "digital" and e.value == 1.0
                       and re.match(r'RDY_KSU\d+_IC_d$', e.channel)]
        rdy_nums = set()
        for c in rdy_channels:
            m = re.search(r'KSU(\d+)', c)
            if m:
                rdy_nums.add(int(m.group(1)))
        if rdy_nums == {1, 2, 3}:
            return {"case": 6, "description": ", ".join(sorted(c.replace("_IC_d","") for c in rdy_channels)),
                    "fault": f"\"{'/'.join(sorted(c.replace('_IC_d','') for c in rdy_channels))}\" came together. Check common source.", "confidence": 0.85}

        int_fc_channels = [e.channel for e in first.events if e.event_type == "digital" and e.value == 1.0
                          and re.match(r'INT_(IC|IN)_FC\d+_d$', e.channel)]
        fc_nums = set()
        for c in int_fc_channels:
            m = re.search(r'FC(\d+)', c)
            if m:
                fc_nums.add(int(m.group(1)))
        if len(fc_nums) >= 2:
            return {"case": 6, "description": ", ".join(sorted(c.replace("_d","") for c in int_fc_channels)),
                    "fault": "Same group interlocks came together.", "confidence": 0.85}

        # Rule 7: Multi-digital different group
        digital_events = [e for e in first.events if e.event_type == "digital" and e.value == 1.0]
        if len(digital_events) >= 2:
            channel_groups = {}
            for e in digital_events:
                ch = e.channel
                if re.match(r'INT_MIS(1|2)_IC_d$', ch):
                    g = "MIS"
                elif re.match(r'INT_PSI(1|2)_IC_d$', ch):
                    g = "PSI"
                elif re.match(r'RDY_KSU\d+_IC_d$', ch):
                    g = "RDY_KSU"
                elif re.match(r'INT_(IC|IN)_FC\d+_d$', ch):
                    g = "INT_FC"
                else:
                    g = "OTHER"
                channel_groups.setdefault(g, []).append(ch.replace("_d", "").replace("_IC_d", ""))
            if len(channel_groups) >= 2:
                all_ch = []
                for gc in channel_groups.values():
                    all_ch.extend(gc)
                return {"case": 7, "description": f"Multiple digital: {', '.join(sorted(all_ch))}",
                        "fault": "Several different interlocks came together. Severe noise seems like a fault source.",
                        "confidence": 0.80}

        # Rule 8: Cavity highhigh
        hh_channels = get_channels_with_event(first, "Cavity_SRF", "highhigh")
        if hh_channels:
            cn = format_channel_names(hh_channels)
            desc = f"{cn} is highhigh" if len(hh_channels) == 1 else f"{cn} are highhigh"
            fm = format_fault_with_numbers("Cavity# blip. Check Cavity.", hh_channels)
            return {"case": 8, "description": desc, "fault": f"'{cn}' → {fm}", "confidence": 0.80}

        # Rule 9: Cavity lowlow
        ll_channels = get_channels_with_event(first, "Cavity_SRF", "lowlow")
        if ll_channels:
            cn = format_channel_names(ll_channels)
            desc = f"{cn} is lowlow" if len(ll_channels) == 1 else f"{cn} are lowlow"
            fm = format_fault_with_numbers("old_Quench_CM#", ll_channels)
            return {"case": 9, "description": desc, "fault": f"'{cn}' → {fm}", "confidence": 0.80}

        # Rule 10: Forward low
        fwd_channels = get_channels_with_event(first, "Forward_SRF", "low")
        if fwd_channels:
            cn = format_channel_names(fwd_channels)
            desc = f"{cn} is low" if len(fwd_channels) == 1 else f"{cn} are low"
            fm = format_fault_with_numbers("RF station# moved first. Check RF station# path", fwd_channels)
            return {"case": 10, "description": desc, "fault": f"'{cn}' → {fm}", "confidence": 0.75}

        # Rule 13: All three Cavity low + BeamCurrent lowlow in THIRD
        cavity_low_all = [c for c in ["Cavity_SRF1_v", "Cavity_SRF2_v", "Cavity_SRF3_v"]
                         if has_in_group(first, c, "low")]
        beam_third = has_in_group(third, "BeamCurrent_v", "lowlow")
        if len(cavity_low_all) == 3 and beam_third:
            cn = ", ".join(c.replace("_v", "") for c in cavity_low_all)
            return {"case": 13, "description": f"{cn} are low",
                    "fault": f"'{cn}' → All RF station moved together. Check common RF source like master oscillator..",
                    "confidence": 0.80}

        # Rule 11: Cavity low (general)
        cav_channels = get_channels_with_event(first, "Cavity_SRF", "low")
        if cav_channels:
            cn = format_channel_names(cav_channels)
            desc = f"{cn} is low" if len(cav_channels) == 1 else f"{cn} are low"
            fm = format_fault_with_numbers("RF station# moved first. Check RF station# path", cav_channels)
            return {"case": 11, "description": desc, "fault": f"'{cn}' → {fm}", "confidence": 0.75}

        # Rule 12: Cavity high
        hi_channels = get_channels_with_event(first, "Cavity_SRF", "highhigh")
        if hi_channels:
            cn = format_channel_names(hi_channels)
            desc = f"{cn} is high" if len(hi_channels) == 1 else f"{cn} are high"
            fm = format_fault_with_numbers("The Cavity# was detuned. Check RF path of Cavity#.", hi_channels)
            return {"case": 12, "description": desc, "fault": f"'{cn}' → {fm}", "confidence": 0.70}

        return {"case": 0, "description": "No matching pattern found",
                "fault": "Unknown fault pattern", "confidence": 0.0}


# -----------------------------------------------------------------------------
# Main classifier class
# -----------------------------------------------------------------------------


class AcceleratorEventClassifier:
    """
    Rule-based event classifier for the monitoring pipeline.

    Replaces the original KMeans-based classifier. Maintains compatibility
    with event_labeller.py by providing the same interface.
    """

    def __init__(self, n_clusters: Optional[int] = None, auto_k: bool = True,
                 ruptures_pen: float = 1.0, config=None):
        if config is not None:
            self._app_config = config
        else:
            self._app_config = get_config()
        self.n_clusters = n_clusters
        self.auto_k = auto_k
        self.ruptures_pen = ruptures_pen
        self.rule_engine = _InternalRuleEngine()
        self.detector = EventDetector()
        self.grouper = EventGrouper()
        self.logger = get_logger(__name__)

    def run(self, input_dir: str, output_dir: str) -> pd.DataFrame:
        """Main entry point - processes all parquet files in input_dir."""
        input_path = Path(input_dir)
        files = sorted(input_path.glob("*.parquet"))

        if not files:
            self.logger.error("No parquet files found.")
            return pd.DataFrame()

        # Read with polars first to handle timezone-aware columns, then convert to pandas
        import polars as pl
        sample_pl = pl.read_parquet(files[0])
        if "event_timestamp" in sample_pl.columns:
            sample_pl = sample_pl.drop("event_timestamp")
        sample_df = sample_pl.to_pandas()
        analog_cols, digital_cols = classify_columns(sample_df.columns.tolist(), sample_df)

        self.logger.info(f"Analyzing {len(files)} files with rule-based classifier...")
        self.logger.info(f"Analog: {len(analog_cols)}ch, Digital: {len(digital_cols)}ch")

        results = []
        all_event_points = {}
        all_sequence_info = {}

        for i, f in enumerate(files):
            print(f"[{i+1}/{len(files)}] Classifying: {f.name}", flush=True)
            self.logger.info(f"[{i+1}/{len(files)}] Processing: {f.name}")
            file_result, event_points, sequence_info = self._classify_file(f, analog_cols, digital_cols)

            if file_result:
                results.append(file_result)
                all_event_points[f.name] = event_points
                all_sequence_info[f.name] = sequence_info

        if not results:
            self.logger.error("No valid results generated.")
            return pd.DataFrame()

        result_df = pd.DataFrame(results)

        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        result_df.to_csv(out_path / "clustering_results.csv", index=False)

        with open(out_path / "event_points.json", "w", encoding="utf-8") as f:
            json.dump(all_event_points, f, ensure_ascii=False, indent=2)

        with open(out_path / "sequence_info.json", "w", encoding="utf-8") as f:
            json.dump(all_sequence_info, f, ensure_ascii=False, indent=2)

        overlay_data = {}
        for fname, seq_info in all_sequence_info.items():
            overlay_data[fname] = {
                "symptom_t": {},
                "symptom_delta": {},
                "collapse_t": {},
                "collapse_slope_line": {},
                "collapse_delta": {},
                "digital_events": []
            }
            first_events = seq_info.get("first_events", [])
            for event in first_events:
                if "digital" in event.get("type", ""):
                    overlay_data[fname]["digital_events"].append({
                        "channel": event["channel"],
                        "time_raw": event["time_raw"],
                        "time_effective": event["time_effective"],
                        "value": event["value"],
                        "delay_compensated": True
                    })

        with open(out_path / "overlay_features.json", "w", encoding="utf-8") as f:
            json.dump(overlay_data, f, ensure_ascii=False, indent=2)

        self.logger.info(f"Rule-based classification complete!")
        self.logger.info(f"Results saved to: {out_path}")
        self.logger.info(f"Files classified: {len(results)}")

        if "case_str" in result_df.columns:
            case_counts = result_df["case"].value_counts().sort_index()
            self.logger.info("Case Distribution:")
            for case_int, count in case_counts.items():
                desc = result_df[result_df["case"] == case_int]["description"].iloc[0]
                self.logger.info(f"  Case {case_int}: {count} files - {desc}")
        else:
            case_counts = result_df["case"].value_counts().sort_index()
            self.logger.info("Case Distribution:")
            for case, count in case_counts.items():
                desc = result_df[result_df["case"] == case]["description"].iloc[0]
                self.logger.info(f"  Case {case}: {count} files - {desc}")

        return result_df

    def _classify_file(self, file_path: Path, analog_cols: List[str],
                      digital_cols: List[str]) -> Tuple[Optional[Dict], List, Dict]:
        """Classify a single parquet file."""
        try:
            import polars as pl
            df_pl = pl.read_parquet(file_path)
            if "event_timestamp" in df_pl.columns:
                df_pl = df_pl.drop("event_timestamp")
            df = df_pl.to_pandas()
            if TIME_COL not in df.columns:
                self.logger.warning(f"Missing {TIME_COL} column in {file_path.name}")
                return None, [], {}

            t = df[TIME_COL].to_numpy(dtype=float)

            all_events = []

            for col in analog_cols:
                if col in df.columns and not col.startswith("Reflect"):
                    events = self.detector.detect_analog_events(df, col, t)
                    all_events.extend(events)

            for col in digital_cols:
                if col in df.columns:
                    events = self.detector.detect_digital_events(df, col, t)
                    all_events.extend(events)

            all_events.sort(key=lambda e: e.effective_time)
            first_group, second_group, third_group = self.grouper.group_events(all_events)

            rule_result = self.rule_engine.apply_rules(
                first_group, second_group, third_group, all_events
            )

            rule_result_compat = rule_result.copy()
            case_int = rule_result.get("case", 0)
            case_str = str(case_int)
            rule_result_compat["case"] = case_int
            rule_result_compat["case_str"] = case_str

            event_points = []
            for i, event in enumerate(all_events):
                etype = "digital" if event.event_type == "digital" else "collapse"
                event_points.append({
                    "order": i + 1,
                    "time_raw": event.time,
                    "time_effective": event.effective_time,
                    "channel": event.channel,
                    "type": etype,
                    "value": event.value,
                    "delay_compensated": event.event_type == "digital"
                })

            sequence_info = {
                "first_events": [
                    {"channel": e.channel, "time_raw": e.time,
                     "time_effective": e.effective_time, "type": e.event_type, "value": e.value}
                    for e in first_group.events
                ],
                "second_events": [
                    {"channel": e.channel, "time_raw": e.time,
                     "time_effective": e.effective_time, "type": e.event_type, "value": e.value}
                    for e in second_group.events
                ],
                "third_events": [
                    {"channel": e.channel, "time_raw": e.time,
                     "time_effective": e.effective_time, "type": e.event_type, "value": e.value}
                    for e in third_group.events
                ],
                "classification": rule_result_compat
            }

            result_row = {
                "filename": file_path.name,
                "cluster": case_int,
                "case": case_int,
                "case_str": case_str,
                "description": rule_result["description"],
                "fault": rule_result["fault"],
                "confidence": rule_result["confidence"],
                "first_group_count": len(first_group.events),
                "second_group_count": len(second_group.events),
                "third_group_count": len(third_group.events),
            }

            return result_row, event_points, sequence_info

        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {e}")
            import traceback
            traceback.print_exc()
            return None, [], {}


# -----------------------------------------------------------------------------
# CLI entry point
# -----------------------------------------------------------------------------

def main():
    """Command-line interface."""
    parser = argparse.ArgumentParser(
        description="SRF Event Pattern Classifier (Rule-Based)"
    )
    parser.add_argument("--input", type=str, required=True,
                        help="Input directory containing parquet files")
    parser.add_argument("--output", type=str, default="results",
                        help="Output directory for results (default: results)")
    parser.add_argument("--verbose", action="store_true",
                        help="Enable verbose logging")
    parser.add_argument("--debug", action="store_true",
                        help="Enable debug logging (show rule application details)")
    parser.add_argument("--n_clusters", type=int, default=0,
                        help="Compatibility parameter (unused)")
    parser.add_argument("--ruptures_pen", type=float, default=1.0,
                        help="Compatibility parameter (unused)")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.debug else (logging.INFO if args.verbose else logging.WARNING)
    logging.basicConfig(level=log_level, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    clf = AcceleratorEventClassifier(
        n_clusters=args.n_clusters if args.n_clusters > 0 else None,
        auto_k=(args.n_clusters == 0),
        ruptures_pen=args.ruptures_pen,
    )

    clf.run(args.input, args.output)


if __name__ == "__main__":
    main()
