"""
event_pattern_classifier.py - SRF Event Pattern Classifier (Rule-Based v4.0)

Rule-based classification derived from the logic diagram.
Key features:
- Signal variation detection: low(<0.9), lowlow(<0.5), highhigh(>1.4)
- 0.01ms time grouping for FIRST/SECOND/THIRD assignment
- 13 specific cases with fault assignments
- 0.4ms digital delay compensation
- Digital noise filtering for INT_FC/INT_MIS channels (1→0 noise bursts)
- CM channels (RDY_CM/QUEN_CM/VAC_CM/ARC_CM/HE-PR_CM) 1→0 transitions are KEPT
  (these channels sit at HIGH=OK normally, dropping to LOW=fault IS the real signal)
"""

from __future__ import annotations

import re
import json
import warnings
from typing import Optional, Dict, List, Tuple, Any

import numpy as np
import pandas as pd

warnings.filterwarnings("ignore")

# ============================================================================
# Configuration from logic diagram
# ============================================================================

TIME_COL = "t_rel_s"

# Signal variation thresholds
HIGHHIGH_THRESH = 1.4       # highhigh > 1.4
HIGH_THRESH = 1.1           # high > 1.1 (above nominal, indicates detuning)
LOW_THRESH = 0.9            # 0.5 < low < 0.9
LOWLOW_THRESH = 0.5         # lowlow < 0.5

# Time grouping (0.01ms for strict simultaneity)
SIMULTANEOUS_MS = 0.01      # signals within 0.01ms are considered simultaneous
SIMULTANEOUS_S = SIMULTANEOUS_MS / 1000.0

# Search window
SEARCH_START_S = -0.045     # -45ms
SEARCH_END_S = 0.050        # +50ms

# Digital delay compensation (0.4ms)
DIGITAL_DELAY_COMPENSATION_S = 0.0004  # 0.4ms

# Digital noise filter: signal must persist for at least 0.01ms
DIGITAL_MIN_PERSISTENCE_S = 0.00001    # 0.01ms

# Analog channel list
ANALOG_COLS = [
    "BeamCurrent_v",
    "Forward_SRF1_v", "Forward_SRF2_v", "Forward_SRF3_v",
    "Cavity_SRF1_v", "Cavity_SRF2_v", "Cavity_SRF3_v",
]

# Digital channel list
DIGITAL_COLS = [
    "INT_MIS1_IC_d", "INT_MIS2_IC_d",
    "INT_IN_FC3_d", "INT_IC_FC2_d", "INT_IC_FC1_d",
    "RDY_KSU3_IC_d", "RDY_KSU2_IC_d", "RDY_KSU1_IC_d",
    "INT_PSI2_IC_d", "INT_PSI1_IC_d",
    "RDY_CM3_FC3_d", "RDY_CM2_FC2_d", "RDY_CM1_FC1_d",
    "HE-PR_CM3_FC3_d", "HE-PR_CM2_FC2_d", "HE-PR_CM1_FC1_d",
    "ARC_CM3_FC3_d", "ARC_CM2_FC2_d", "ARC_CM1_FC1_d",
    "QUEN_CM3_FC3_d", "QUEN_CM2_FC2_d", "QUEN_CM1_FC1_d",
    "VAC_CM3_FC3_d", "VAC_CM2_FC2_d", "VAC_CM1_FC1_d",
]

# ============================================================================
# Core data structures
# ============================================================================

class SignalEvent:
    """Represents a detected signal variation event."""

    def __init__(self, time: float, channel: str, event_type: str, value: float):
        self.time = time          # absolute time in seconds (raw measurement)
        self.channel = channel    # channel name
        self.event_type = event_type  # "low", "lowlow", "highhigh", "high", "digital"
        self.value = value        # signal value at event time

    @property
    def effective_time(self) -> float:
        """Time used for ordering - compensated for digital delay."""
        if self.event_type == "digital":
            return self.time - DIGITAL_DELAY_COMPENSATION_S
        return self.time

    def __repr__(self):
        return f"SignalEvent({self.channel}, t={self.time*1000:.2f}ms, {self.event_type})"

    def __lt__(self, other):
        return self.effective_time < other.effective_time


class TimeGroup:
    """Represents FIRST, SECOND, THIRD time groups."""

    def __init__(self, group_name: str, start_time: float = 0.0):
        self.name = group_name  # "FIRST", "SECOND", "THIRD"
        self.start_time = start_time
        self.events: List[SignalEvent] = []

    def add_event(self, event: SignalEvent):
        self.events.append(event)

    def get_channels(self) -> List[str]:
        return [e.channel for e in self.events]

    def get_event_types(self) -> List[str]:
        return [e.event_type for e in self.events]

    def has_channel(self, channel: str) -> bool:
        return any(e.channel == channel for e in self.events)

    def has_event_type(self, event_type: str) -> bool:
        return any(e.event_type == event_type for e in self.events)

    def get_channel_event_pairs(self) -> List[Tuple[str, str]]:
        return [(e.channel, e.event_type) for e in self.events]

    def __repr__(self):
        return f"TimeGroup({self.name}, {len(self.events)} events, start={self.start_time*1000:.2f}ms)"


# ============================================================================
# Event detection
# ============================================================================

def detect_analog_events(df: pd.DataFrame, channel: str, t: np.ndarray) -> List[SignalEvent]:
    """Detect low, lowlow, highhigh, high events in analog channel."""
    if channel.startswith("Reflect"):
        return []

    events = []
    if channel not in df.columns:
        return events

    vals = df[channel].to_numpy(dtype=float)

    mask = (t >= SEARCH_START_S) & (t <= SEARCH_END_S)
    if not np.any(mask):
        return events

    window_vals = vals[mask]
    window_t = t[mask]

    # lowlow (< 0.5)
    lowlow_mask = window_vals < LOWLOW_THRESH
    if np.any(lowlow_mask):
        first_idx = np.argmax(lowlow_mask)
        events.append(SignalEvent(
            time=float(window_t[first_idx]),
            channel=channel,
            event_type="lowlow",
            value=float(window_vals[first_idx]),
        ))

    # low (0.5 < low < 0.9)
    low_mask = (window_vals > 0.5) & (window_vals < LOW_THRESH)
    if np.any(low_mask):
        first_idx = np.argmax(low_mask)
        events.append(SignalEvent(
            time=float(window_t[first_idx]),
            channel=channel,
            event_type="low",
            value=float(window_vals[first_idx]),
        ))

    # high (HIGH_THRESH < high < HIGHHIGH_THRESH — above nominal, indicating detuning)
    high_mask = (window_vals > HIGH_THRESH) & (window_vals < HIGHHIGH_THRESH)
    if np.any(high_mask):
        first_idx = np.argmax(high_mask)
        events.append(SignalEvent(
            time=float(window_t[first_idx]),
            channel=channel,
            event_type="high",
            value=float(window_vals[first_idx]),
        ))

    # highhigh (> 1.4)
    highhigh_mask = window_vals > HIGHHIGH_THRESH
    if np.any(highhigh_mask):
        first_idx = np.argmax(highhigh_mask)
        events.append(SignalEvent(
            time=float(window_t[first_idx]),
            channel=channel,
            event_type="highhigh",
            value=float(window_vals[first_idx]),
        ))

    return events


def detect_digital_events(df: pd.DataFrame, channel: str, t: np.ndarray) -> List[SignalEvent]:
    """
    Detect digital signal changes (0→1 and 1→0 transitions) with noise filtering.
    For 0→1 or 1→0: signal must persist for at least DIGITAL_MIN_PERSISTENCE_S (0.01ms).

    Noise filtering rules:
    - INT_FC channels (INT_IC_FC, INT_IN_FC): filter 1→0 transitions (noise bursts)
    - INT_MIS channels: filter 1→0 transitions (noise bursts)
    - CM channels (RDY_CM, QUEN_CM, VAC_CM, ARC_CM, HE-PR_CM):
      DO NOT filter 1→0 transitions — these channels are normally HIGH(1)=OK
      and drop to LOW(0)=fault, so 1→0 IS the real interlock signal.
    """
    events = []
    if channel not in df.columns:
        return events

    vals = df[channel].to_numpy(dtype=float)

    mask = (t >= SEARCH_START_S) & (t <= SEARCH_END_S)
    if not np.any(mask):
        return events

    window_vals = vals[mask]
    window_t = t[mask]

    if len(window_t) < 2:
        return events

    dt = np.mean(np.diff(window_t))
    min_samples_needed = max(1, int(np.ceil(DIGITAL_MIN_PERSISTENCE_S / dt - 0.0005)))

    diff = np.diff(window_vals)
    change_indices = np.where(diff != 0)[0]

    if len(change_indices) == 0:
        return events

    change_indices = list(change_indices) + [len(window_vals) - 1]

    for i in range(len(change_indices) - 1):
        ci = change_indices[i]
        nci = change_indices[i + 1]
        new_value = window_vals[ci + 1]
        change_time = window_t[ci + 1]

        if nci == len(window_vals) - 1:
            consecutive_samples = len(window_vals) - (ci + 1)
        else:
            consecutive_samples = nci - ci

        if consecutive_samples < min_samples_needed:
            continue

        # Noise filtering: INT_FC channels — filter 1→0 (noise bursts)
        if re.match(r'INT_(IC|IN)_FC\d+_d$', channel) and new_value == 0.0:
            continue

        # Noise filtering: INT_MIS channels — filter 1→0 (noise bursts)
        if re.match(r'INT_MIS(1|2)_IC_d$', channel) and new_value == 0.0:
            continue

        # CM channels (RDY_CM, QUEN_CM, VAC_CM, ARC_CM, HE-PR_CM):
        # These are normally HIGH(1)=OK, dropping to LOW(0)=fault.
        # The 1→0 transition IS the real signal — do NOT filter it.

        events.append(SignalEvent(
            time=float(change_time),
            channel=channel,
            event_type="digital",
            value=new_value,
        ))

    return events


# ============================================================================
# Time grouping
# ============================================================================

def group_events_by_time(all_events: List[SignalEvent]) -> Tuple[TimeGroup, TimeGroup, TimeGroup]:
    """
    Group events into FIRST, SECOND, THIRD:
    1. Earliest event is FIRST
    2. Events within SIMULTANEOUS_MS of first event are also FIRST
    3. THIRD = BeamCurrent_lowlow (always exists, scope trigger)
    4. Events between FIRST and THIRD are SECOND
    """
    first_group = TimeGroup("FIRST", 0.0)
    second_group = TimeGroup("SECOND", 0.0)
    third_group = TimeGroup("THIRD", 0.0)

    if not all_events:
        return first_group, second_group, third_group

    sorted_events = sorted(all_events, key=lambda e: e.effective_time)

    # Find BeamCurrent_lowlow for THIRD
    third_event = None
    for event in sorted_events:
        if event.channel == "BeamCurrent_v" and event.event_type == "lowlow":
            third_event = event
            break

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
            time_diff_ms = abs(event.effective_time - first_group.start_time) * 1000
            if time_diff_ms <= SIMULTANEOUS_MS:
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


# ============================================================================
# Helper functions for rule engine
# ============================================================================

def _is_valid_interlock(e: SignalEvent) -> bool:
    """Check if a digital event represents a valid interlock signal.
    Only 0→1 transitions (value=1.0) are valid interlocks for the rule engine.
    1→0 transitions (value=0.0) on CM channels (RDY_CM, QUEN_CM, VAC_CM, ARC_CM,
    HE-PR_CM) are kept during detection for correct FIRST group ordering, but
    they are NOT counted as "interlock signals" for rule matching.
    """
    if e.event_type != "digital":
        return False
    if e.value == 1.0:
        return True
    return False


def has_in_group(group: TimeGroup, channel: str, event_type: str = None) -> bool:
    for e in group.events:
        if e.channel == channel:
            if event_type is None or e.event_type == event_type:
                if event_type == "digital" and not _is_valid_interlock(e):
                    continue
                return True
    return False


def get_channels_with_event(group: TimeGroup, prefix: str, event_type: str = None) -> List[str]:
    result = []
    for e in group.events:
        if e.channel.startswith(prefix):
            if event_type is None or e.event_type == event_type:
                if event_type == "digital" and not _is_valid_interlock(e):
                    continue
                result.append(e.channel)
    return result


def extract_srf_number(channel: str) -> str:
    m = re.search(r'SRF(\d+)', channel)
    return m.group(1) if m else "#"


def format_channel_names(channels: List[str]) -> str:
    return ", ".join(c.replace("_v", "") for c in channels) if channels else ""


def format_fault_with_numbers(fault_template: str, channels: List[str]) -> str:
    if not channels:
        return fault_template
    numbers = [extract_srf_number(ch) for ch in channels if extract_srf_number(ch) != "#"]
    if not numbers:
        return fault_template
    numbers_str = ",".join(sorted(set(numbers)))
    return fault_template.replace("#", numbers_str)


# ============================================================================
# Rule descriptions and fault assignments
# ============================================================================

CASE_DESCRIPTIONS = {
    1: "BeamCurrent is low",
    2: "BeamCurrent is lowlow",
    3: "Any single digital",
    4: "INT_MIS1_IC or INT_MIS2_IC",
    5: "INT_PSI1_IC or INT_PSI2_IC",
    6: "Multi-digital(same group)",
    7: "Multi-digital(different group)",
    8: "Cavity_SRF# is highhigh",
    9: "Cavity_SRF# is lowlow",
    10: "Forward_SRF# is low",
    11: "Cavity_SRF# is low",
    12: "Cavity_SRF# is high",
    13: "Cavity_SRF1 is low & Cavity_SRF2 is low & Cavity_SRF3 is low",
}

FAULT_ASSIGNMENTS = {
    1: "Beam loss, check other systems like MPS, feedback system.",
    2: "Beam loss, check other systems like MPS, feedback system.",
    3: '"the first digital interlock" is the fault',
    4: "INT_MIS_IC is the fault.",
    5: "INT_PSI_IC is the fault.",
    6: '"same group" came together. Check common source of "same group" and check MIS interlock.',
    7: "Several different interlocks came together. Severe noise seems like a fault source. Check common fault of \"digital interlocks\".",
    8: "Cavity# blip. Check Cavity#.",
    9: "old_Quench_CM#",
    10: "RF station# moved first. Check RF station# path",
    11: "RF station# moved first. Check RF station# path",
    12: "The Cavity# was detuned. Check RF path of Cavity#.",
    13: "All RF station moved together. Check common RF source like master oscillator,...",
}


# ============================================================================
# Rule engine
# ============================================================================

def apply_rules(first: TimeGroup, second: TimeGroup, third: TimeGroup,
                all_events: List[SignalEvent]) -> Dict[str, Any]:
    """Apply rules in order (priority per classification table), return first match.

    Priority order (from classification table):
      1. BeamCurrent low        -> Case 1
      2. BeamCurrent lowlow      -> Case 2
      3. INT_MIS                 -> Case 4  (before single digital)
      4. INT_PSI                 -> Case 5  (before single digital)
      5. Single digital/event    -> Case 3  (except MIS & PSI)
      6. Multi-digital(same)     -> Case 6
      7. Multi-digital(diff)     -> Case 7
      8. Cavity_SRF highhigh     -> Case 8
      9. Cavity_SRF lowlow       -> Case 9
     10. Forward_SRF low         -> Case 10
     11. All 3 Cavity low        -> Case 13 (specific before general)
     12. Cavity_SRF low (gen)    -> Case 11
     13. Cavity_SRF high         -> Case 12
    """

    # Rule 1: BeamCurrent low in FIRST
    if has_in_group(first, "BeamCurrent_v", "low"):
        return {"case": 1, "description": "BeamCurrent is low", "fault": FAULT_ASSIGNMENTS[1], "confidence": 0.95}

    # Rule 2: BeamCurrent lowlow in FIRST
    if has_in_group(first, "BeamCurrent_v", "lowlow"):
        return {"case": 2, "description": "BeamCurrent is lowlow", "fault": FAULT_ASSIGNMENTS[2], "confidence": 0.95}

    # Collect valid interlock events in FIRST
    digital_first = [e for e in first.events if _is_valid_interlock(e)]

    # Rule 4: INT_MIS (before single digital)
    mis = [e for e in digital_first if re.match(r'INT_MIS(1|2)_IC_d$', e.channel)]
    if mis:
        other = [e for e in digital_first if not re.match(r'INT_MIS(1|2)_IC_d$', e.channel)]
        if not other:
            names = ", ".join(sorted(c.replace("_d", "") for c in (e.channel for e in mis)))
            return {"case": 4, "description": names, "fault": "INT_MIS_IC is the fault.", "confidence": 0.85}

    # Rule 5: INT_PSI (before single digital)
    psi = [e for e in digital_first if re.match(r'INT_PSI(1|2)_IC_d$', e.channel)]
    if psi:
        other = [e for e in digital_first if not re.match(r'INT_PSI(1|2)_IC_d$', e.channel)]
        if not other:
            names = ", ".join(sorted(c.replace("_d", "") for c in (e.channel for e in psi)))
            return {"case": 5, "description": names, "fault": "INT_PSI_IC is the fault.", "confidence": 0.85}

    # Rule 3: Single digital/event (except MIS & PSI)
    # Single valid digital interlock (value=1.0, excluding MIS/PSI)
    if len(digital_first) == 1:
        ch = digital_first[0].channel
        if not re.match(r'INT_MIS(1|2)_IC_d$', ch) and not re.match(r'INT_PSI(1|2)_IC_d$', ch):
            name = ch.replace("_IC_d", "").replace("_d", "")
            return {"case": 3, "description": f"Single digital: {ch}", "fault": f'"{name}" is the fault', "confidence": 0.90}
    # Fallback: single event of ANY kind (CM 1->0, single analog), excluding MIS/PSI
    if len(first.events) == 1:
        e = first.events[0]
        ch = e.channel
        if not re.match(r'INT_MIS(1|2)_IC_d$', ch) and not re.match(r'INT_PSI(1|2)_IC_d$', ch):
            name = ch.replace("_v", "").replace("_d", "")
            return {"case": 3, "description": f"Single event: {e.channel}", "fault": f'"{name}" is the fault', "confidence": 0.90}

    # Rule 6: Multi-digital(same group)
    # 6a: RDY_KSU 1,2,3 together
    rdy_nums = set()
    for e in digital_first:
        m = re.match(r'RDY_KSU(\d+)_IC_d$', e.channel)
        if m:
            rdy_nums.add(int(m.group(1)))
    if rdy_nums == {1, 2, 3}:
        other = [e for e in digital_first
                 if not re.match(r'RDY_KSU\d+_IC_d$', e.channel)
                 and not re.match(r'INT_(IC|IN)_FC\d+_d$', e.channel)]
        if not has_in_group(first, "BeamCurrent_v") and not other:
            names = ", ".join(sorted(f"RDY_KSU{n}_IC" for n in sorted(rdy_nums)))
            return {"case": 6, "description": names, "fault": FAULT_ASSIGNMENTS[6], "confidence": 0.85}
    # 6b: 2+ INT_FC channels together
    fc_nums = set()
    for e in digital_first:
        m = re.search(r'FC(\d+)', e.channel)
        if m and re.match(r'INT_(IC|IN)_FC\d+_d$', e.channel):
            fc_nums.add(int(m.group(1)))
    if len(fc_nums) >= 2:
        names = ", ".join(sorted(
            c.replace("_d", "") for c in (e.channel for e in digital_first
            if re.match(r'INT_(IC|IN)_FC\d+_d$', e.channel))
        ))
        return {"case": 6, "description": names, "fault": FAULT_ASSIGNMENTS[6], "confidence": 0.85}

    # Rule 7: Multi-digital(different groups)
    if len(digital_first) >= 2:
        groups = {}
        for e in digital_first:
            ch = e.channel
            if re.match(r'INT_MIS(1|2)_IC_d$', ch):
                g = "MIS"
            elif re.match(r'INT_PSI(1|2)_IC_d$', ch):
                g = "PSI"
            elif re.match(r'RDY_KSU\d+_IC_d$', ch):
                g = "RDY_KSU"
            elif re.match(r'INT_(IC|IN)_FC\d+_d$', ch):
                g = "INT_FC"
            elif re.match(r'.*_CM\d+_FC\d+_d$', ch):
                g = "CM"
            else:
                g = "OTHER"
            groups.setdefault(g, []).append(ch.replace("_d", "").replace("_IC_d", ""))
        if len(groups) >= 2:
            names = ", ".join(sorted(v for vs in groups.values() for v in vs))
            return {
                "case": 7,
                "description": f"Multiple digital: {names}",
                "fault": FAULT_ASSIGNMENTS[7],
                "confidence": 0.80,
            }

    # Rule 8: Cavity_SRF highhigh
    chs = get_channels_with_event(first, "Cavity_SRF", "highhigh")
    if chs:
        desc = f"{format_channel_names(chs)} is highhigh"
        fault = f"'{format_channel_names(chs)}' is highhigh -> Cavity# blip. Check Cavity."
        return {"case": 8, "description": desc, "fault": fault, "confidence": 0.80}

    # Rule 9: Cavity_SRF lowlow
    chs = get_channels_with_event(first, "Cavity_SRF", "lowlow")
    if chs:
        desc = f"{format_channel_names(chs)} is lowlow"
        fault = f"'{format_channel_names(chs)}' is lowlow -> old_Quench_CM#"
        return {"case": 9, "description": desc, "fault": fault, "confidence": 0.80}

    # Rule 10: Forward_SRF low
    chs = get_channels_with_event(first, "Forward_SRF", "low")
    if chs:
        desc = f"{format_channel_names(chs)} is low"
        fault = f"'{format_channel_names(chs)}' is low -> RF station# moved first. Check RF station# path"
        return {"case": 10, "description": desc, "fault": fault, "confidence": 0.75}

    # Rule 13: All 3 Cavity low (specific before general)
    all_cav_low = all(has_in_group(first, f"Cavity_SRF{n}_v", "low") for n in ["1", "2", "3"])
    beam_lowlow_third = has_in_group(third, "BeamCurrent_v", "lowlow")
    if all_cav_low and beam_lowlow_third:
        return {
            "case": 13,
            "description": "Cavity_SRF1, Cavity_SRF2, Cavity_SRF3 are low",
            "fault": "All RF station moved together. Check common RF source like master oscillator,...",
            "confidence": 0.80,
        }

    # Rule 11: Cavity_SRF low (general)
    chs = get_channels_with_event(first, "Cavity_SRF", "low")
    if chs:
        desc = f"{format_channel_names(chs)} is low"
        fault = f"'{format_channel_names(chs)}' is low -> RF station# moved first. Check RF station# path"
        return {"case": 11, "description": desc, "fault": fault, "confidence": 0.75}

    # Rule 12: Cavity_SRF high (detuning indicator)
    chs = get_channels_with_event(first, "Cavity_SRF", "high")
    if chs:
        desc = f"{format_channel_names(chs)} is high"
        fault = f"'{format_channel_names(chs)}' is high -> The Cavity# was detuned. Check RF path of Cavity#."
        return {"case": 12, "description": desc, "fault": fault, "confidence": 0.70}

    return {"case": 0, "description": "No matching pattern found", "fault": "Unknown fault pattern", "confidence": 0.0}


# ============================================================================
# Public API
# ============================================================================

def classify_event(df: pd.DataFrame) -> Dict[str, Any]:
    """Classify a single event from its merged parquet DataFrame.

    Args:
        df: pandas DataFrame from a merged parquet file.
            Must contain t_rel_s column.

    Returns:
        dict with case, case_str, case_description, case_fault,
        case_confidence, events_count, time_groups.
    """
    t = df[TIME_COL].to_numpy(dtype=float)

    all_events: List[SignalEvent] = []

    # Analog detection
    for col in ANALOG_COLS:
        if col in df.columns:
            all_events.extend(detect_analog_events(df, col, t))

    # Digital detection
    for col in DIGITAL_COLS:
        if col in df.columns:
            all_events.extend(detect_digital_events(df, col, t))

    all_events.sort(key=lambda e: e.effective_time)
    first, second, third = group_events_by_time(all_events)

    result = apply_rules(first, second, third, all_events)

    # Serialize time groups for DB storage
    def _serialize_group(g: TimeGroup) -> dict:
        return {
            "name": g.name,
            "start_time_s": round(g.start_time, 7),
            "events": [
                {
                    "channel": e.channel,
                    "event_type": e.event_type,
                    "time_s": round(e.time, 7),
                    "effective_time_s": round(e.effective_time, 7),
                    "value": e.value,
                }
                for e in g.events
            ],
        }

    time_groups = {
        "first": _serialize_group(first),
        "second": _serialize_group(second),
        "third": _serialize_group(third),
    }

    return {
        "case": result["case"],
        "case_str": str(result["case"]),
        "case_description": result["description"],
        "case_fault": result["fault"],
        "case_confidence": round(result["confidence"], 2),
        "events_count": len(all_events),
        "time_groups": time_groups,
    }
