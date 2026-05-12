"""
Rule engine for SRF event classification.

Implements the logic diagram rules for classifying events into 13 cases.
"""

from typing import Dict, List, Any, Tuple
import re

from ..core.logger import get_logger
from ..pipeline.datatypes import TimeGroup, SignalEvent


class RuleEngine:
    """Implements the logic diagram rules for classification."""

    def __init__(self):
        self.case_descriptions = {
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
            13: "Cavity_SRF1 is low & Cavity_SRF2 is low & Cavity_SRF3 is low"
        }

        self.fault_assignments = {
            1: "Beam loss, check other systems like MPS, feedback system",
            2: "Beam loss, check other systems like MPS, feedback system",
            3: "\"the first digital interlock\" is the fault",
            4: "INT_MIS_IC is the fault.",
            5: "INT_PSI_IC is the fault.",
            6: "\"same group\" came together. Check common source of \"same group\" and check MIS interlock.",
            7: "Several different interlocks came together. Severe noise seems like a fault source. Check common fault of \"digital interlocks\".",
            8: "Cavity# blip. Check Cavity#.",
            9: "old_Quench_CM#",
            10: "RF station# moved first. Check RF station# path",
            11: "RF station# moved first. Check RF station# path",
            12: "The Cavity# was detuned. Check RF path of Cavity#.",
            13: "All RF station moved together. Check common RF source like master oscillator,.."
        }
        self.logger = get_logger(__name__)
        self.rules = list(self.case_descriptions.keys())  # List of case numbers

    def apply_rules(self, first: TimeGroup, second: TimeGroup, third: TimeGroup,
                   all_events: List[SignalEvent]) -> Dict[str, Any]:
        """
        Apply rules in order, return first matching case.
        Returns dict with case number, description, fault assignment, and confidence.
        """
        # Extract channel-event pairs for each group
        first_pairs = first.get_channel_event_pairs()
        second_pairs = second.get_channel_event_pairs()
        third_pairs = third.get_channel_event_pairs()

        # Get all channels in each group
        first_channels = first.get_channels()
        second_channels = second.get_channels()
        third_channels = third.get_channels()

        # Helper: Check if channel is in group with specific event type
        def has_in_group(group: TimeGroup, channel: str, event_type: str = None) -> bool:
            for e in group.events:
                if e.channel == channel:
                    if event_type is None or e.event_type == event_type:
                        # For digital events, only count if value == 1 (real interlock)
                        if event_type == "digital" and e.value != 1.0:
                            continue  # Skip digital events with value != 1
                        return True
            return False

        # Helper: Count occurrences in group
        def count_in_group(group: TimeGroup, channel_prefix: str, event_type: str = None) -> int:
            count = 0
            for e in group.events:
                if e.channel.startswith(channel_prefix):
                    if event_type is None or e.event_type == event_type:
                        # For digital events, only count if value == 1 (real interlock)
                        if event_type == "digital" and e.value != 1.0:
                            continue
                        count += 1
            return count

        # Helper: Get channel names for specific event type
        def get_channels_with_event(group: TimeGroup, channel_prefix: str, event_type: str = None) -> List[str]:
            channels = []
            for e in group.events:
                if e.channel.startswith(channel_prefix):
                    if event_type is None or e.event_type == event_type:
                        # For digital events, only count if value == 1 (real interlock)
                        if event_type == "digital" and e.value != 1.0:
                            continue
                        channels.append(e.channel)
            return channels

        # Helper: Extract SRF number from channel name (e.g., "Cavity_SRF1_v" -> "1")
        def extract_srf_number(channel_name: str) -> str:
            match = re.search(r'SRF(\d+)', channel_name)
            if match:
                return match.group(1)
            # Fallback: try to extract number after underscore
            parts = channel_name.split('_')
            for part in parts:
                if part.isdigit():
                    return part
            return "#"

        # Helper: Format channel names with SRF numbers
        def format_channel_names(channels: List[str]) -> str:
            if not channels:
                return ""
            # Remove _v suffix and keep SRF number
            formatted = []
            for ch in channels:
                clean = ch.replace("_v", "")
                formatted.append(clean)
            return ", ".join(formatted)

        # Helper: Format fault message with SRF numbers
        def format_fault_with_numbers(fault_template: str, channels: List[str]) -> str:
            if not channels:
                return fault_template

            # Extract SRF numbers from channels
            numbers = []
            for ch in channels:
                num = extract_srf_number(ch)
                if num != "#":
                    numbers.append(num)

            if not numbers:
                return fault_template

            # Replace # with numbers in fault template
            numbers_str = ",".join(sorted(set(numbers)))
            fault_msg = fault_template.replace("#", numbers_str)

            # If multiple numbers, make them plural if needed
            if len(numbers) > 1 and "station#" in fault_msg:
                fault_msg = fault_msg.replace("station" + numbers_str, "stations " + numbers_str)

            return fault_msg

        # Rule 1: BeamCurrent is low in FIRST
        if has_in_group(first, "BeamCurrent_v", "low"):
            return {
                "case": 1,
                "description": "BeamCurrent is low",
                "fault": "Beam loss, check other systems like MPS, feedback system",
                "confidence": 0.95
            }

        # Rule 2: BeamCurrent is lowlow in FIRST
        if has_in_group(first, "BeamCurrent_v", "lowlow"):
            return {
                "case": 2,
                "description": "BeamCurrent is lowlow",
                "fault": "Beam loss, check other systems like MPS, feedback system",
                "confidence": 0.95
            }

        # Rule 3: Any single digital in FIRST (excluding INT_MIS and INT_PSI patterns)
        digital_in_first = [e for e in first.events if e.event_type == "digital" and e.value == 1.0]
        if len(digital_in_first) == 1:
            digital_channel = digital_in_first[0].channel
            # Check if it's INT_MIS or INT_PSI pattern
            is_mis = re.match(r'INT_MIS(1|2)_IC_d$', digital_channel)
            is_psi = re.match(r'INT_PSI(1|2)_IC_d$', digital_channel)

            # Skip if it's MIS or PSI pattern (handled by Rule 4, 5)
            if not is_mis and not is_psi:
                # Extract simple name (remove suffix)
                simple_name = digital_channel.replace("_IC_d", "").replace("_d", "")
                # Format according to user table
                return {
                    "case": 3,
                    "description": f"Single digital: {digital_channel}",
                    "fault": f"\"{simple_name}\" is the fault",
                    "confidence": 0.90
                }

        # Rule 4: INT_MIS1_IC or INT_MIS2_IC in FIRST
        mis_channels = []
        mis_channel_names = []
        for e in first.events:
            if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                channel = e.channel
                if re.match(r'INT_MIS(1|2)_IC_d$', channel):
                    mis_channels.append(channel)
                    clean_name = channel.replace("_d", "")
                    mis_channel_names.append(clean_name)

        if mis_channels:
            # Check if only MIS channels are present (no other digital signals)
            other_digital = False
            for e in first.events:
                if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                    ch = e.channel
                    if not re.match(r'INT_MIS(1|2)_IC_d$', ch):
                        other_digital = True
                        break

            if not other_digital:
                formatted_names = ", ".join(sorted(mis_channel_names))
                return {
                    "case": 4,
                    "description": formatted_names,
                    "fault": "INT_MIS_IC is the fault.",
                    "confidence": 0.85
                }

        # Rule 5: INT_PSI1_IC or INT_PSI2_IC in FIRST
        psi_channels = []
        psi_channel_names = []
        for e in first.events:
            if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                channel = e.channel
                if re.match(r'INT_PSI(1|2)_IC_d$', channel):
                    psi_channels.append(channel)
                    clean_name = channel.replace("_d", "")
                    psi_channel_names.append(clean_name)

        if psi_channels:
            # Check if only PSI channels are present (no other digital signals)
            other_digital = False
            for e in first.events:
                if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                    ch = e.channel
                    if not re.match(r'INT_PSI(1|2)_IC_d$', ch):
                        other_digital = True
                        break

            if not other_digital:
                formatted_names = ", ".join(sorted(psi_channel_names))
                return {
                    "case": 5,
                    "description": formatted_names,
                    "fault": "INT_PSI_IC is the fault.",
                    "confidence": 0.85
                }

        # Rule 5-1: Multi-digital(same group) in FIRST
        # Check for RDY_KSU or INT_FC groups

        # Rule 5-1a: RDY_KSU1 & RDY_KSU2 & RDY_KSU3 in FIRST (without BeamCurrent or other digital)
        rdy_channels_present = []
        rdy_channel_names = []

        # Check for RDY_KSU channels
        for e in first.events:
            if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                channel = e.channel
                # Match RDY_KSU pattern
                if re.match(r'RDY_KSU\d+_IC_d$', channel):
                    rdy_channels_present.append(channel)
                    # Remove suffix for display
                    clean_name = channel.replace("_IC_d", "")
                    rdy_channel_names.append(clean_name)

        # Need exactly 3 unique channels (KSU1, KSU2, KSU3)
        # Extract channel numbers to ensure we have 1,2,3
        channel_numbers = []
        for ch in rdy_channels_present:
            match = re.search(r'KSU(\d+)', ch)
            if match:
                channel_numbers.append(int(match.group(1)))

        # Check if we have all three numbers (1,2,3)
        if set(channel_numbers) == {1, 2, 3}:
            # Verify no BeamCurrent or other digital in FIRST
            beam_in_first = has_in_group(first, "BeamCurrent_v")
            # Check for other digital signals (not RDY_KSU or INT_FC)
            other_digital = False
            for e in first.events:
                if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                    ch = e.channel
                    # Skip if it's RDY_KSU or INT_FC pattern
                    is_rdy = re.match(r'RDY_KSU\d+_IC_d$', ch)
                    is_int_fc = re.match(r'INT_(IC|IN)_FC\d+_d$', ch)
                    if not is_rdy and not is_int_fc:
                        other_digital = True
                        break

            if not beam_in_first and not other_digital:
                # Format channel names for display
                formatted_names = ", ".join(sorted(rdy_channel_names))
                return {
                    "case": 6,
                    "description": f"{formatted_names}",
                    "fault": f"\"{formatted_names}\" came together. Check common source of \"{formatted_names}\" and check MIS interlock.",
                    "confidence": 0.85
                }

        # Rule 5: INT_IC/IN_FC1 & INT_IC/IN_FC2 & INT_IC/IN_FC3 in FIRST
        # Support both INT_IC_FC and INT_IN_FC patterns (file may contain typos)
        int_channels_present = []
        int_channel_names = []

        # Check for INT_*_FC1/2/3 patterns
        for e in first.events:
            if e.event_type == "digital" and e.value == 1.0:  # Only digital with value=1
                channel = e.channel
                # Match INT_IC_FC1_d, INT_IN_FC2_d, etc.
                if re.match(r'INT_(IC|IN)_FC\d+_d$', channel):
                    int_channels_present.append(channel)
                    # Remove suffix for display
                    clean_name = channel.replace("_d", "")
                    int_channel_names.append(clean_name)

        # Need at least 2 unique FC channels (e.g., FC1 & FC3, FC1 & FC2, etc.)
        # Extract channel numbers to ensure we have multiple FC channels
        channel_numbers = []
        for ch in int_channels_present:
            match = re.search(r'FC(\d+)', ch)
            if match:
                channel_numbers.append(int(match.group(1)))

        # Check if we have at least 2 different FC channels
        if len(set(channel_numbers)) >= 2:
            # Format channel names for display
            formatted_names = ", ".join(sorted(int_channel_names))
            return {
                "case": 6,
                "description": f"{formatted_names}",
                "fault": f"\"{formatted_names}\" came together. Check common source of \"{formatted_names}\" and check MIS interlock.",
                "confidence": 0.85
            }

        # Rule 7: Multi-digital(different group) in FIRST
        # Check if there are digital signals from different groups
        digital_events = [e for e in first.events if e.event_type == "digital" and e.value == 1.0]
        if len(digital_events) >= 2:
            # Group digital channels by pattern
            channel_groups = {}
            for e in digital_events:
                ch = e.channel
                # Categorize by pattern
                if re.match(r'INT_MIS(1|2)_IC_d$', ch):
                    group = "MIS"
                elif re.match(r'INT_PSI(1|2)_IC_d$', ch):
                    group = "PSI"
                elif re.match(r'RDY_KSU\d+_IC_d$', ch):
                    group = "RDY_KSU"
                elif re.match(r'INT_(IC|IN)_FC\d+_d$', ch):
                    group = "INT_FC"
                else:
                    group = "OTHER"

                if group not in channel_groups:
                    channel_groups[group] = []
                channel_groups[group].append(ch.replace("_d", "").replace("_IC_d", ""))

            # If we have at least 2 different groups
            if len(channel_groups) >= 2:
                # Format channel names for display
                all_channels = []
                for group_channels in channel_groups.values():
                    all_channels.extend(group_channels)
                formatted_names = ", ".join(sorted(all_channels))

                return {
                    "case": 7,
                    "description": f"Multiple digital: {formatted_names}",
                    "fault": f"Several different interlocks came together. Severe noise seems like a fault source. Check common fault of \"{formatted_names}\".",
                    "confidence": 0.80
                }

        # Rule 8: Cavity_SRF# is highhigh in FIRST
        cavity_highhigh_channels = get_channels_with_event(first, "Cavity_SRF", "highhigh")
        if cavity_highhigh_channels:
            # Format channel names (e.g., "Cavity_SRF1, Cavity_SRF2")
            channel_names = format_channel_names(cavity_highhigh_channels)
            # Format fault message with actual SRF numbers
            fault_msg = format_fault_with_numbers("Cavity# blip. Check Cavity.", cavity_highhigh_channels)

            # Create description with actual channel names
            if len(cavity_highhigh_channels) == 1:
                description = f"{channel_names} is highhigh"
            else:
                description = f"{channel_names} are highhigh"

            return {
                "case": 8,
                "description": description,
                "fault": f"'{channel_names}' is highhigh → {fault_msg}",
                "confidence": 0.80
            }

        # Rule 9: Cavity_SRF# is lowlow in FIRST
        cavity_lowlow_channels = get_channels_with_event(first, "Cavity_SRF", "lowlow")
        if cavity_lowlow_channels:
            channel_names = format_channel_names(cavity_lowlow_channels)
            fault_msg = format_fault_with_numbers("old_Quench_CM#", cavity_lowlow_channels)

            if len(cavity_lowlow_channels) == 1:
                description = f"{channel_names} is lowlow"
            else:
                description = f"{channel_names} are lowlow"

            return {
                "case": 9,
                "description": description,
                "fault": f"'{channel_names}' is lowlow → {fault_msg}",
                "confidence": 0.80
            }

        # Rule 10: Forward_SRF# is low
        forward_low_channels = get_channels_with_event(first, "Forward_SRF", "low")
        if forward_low_channels:
            channel_names = format_channel_names(forward_low_channels)
            fault_msg = format_fault_with_numbers("RF station# moved first. Check RF station# path", forward_low_channels)

            if len(forward_low_channels) == 1:
                description = f"{channel_names} is low"
            else:
                description = f"{channel_names} are low"

            return {
                "case": 10,
                "description": description,
                "fault": f"'{channel_names}' is low → {fault_msg}",
                "confidence": 0.75
            }

        # Rule 13: Cavity_SRF1 is low & Cavity_SRF2 is low & Cavity_SRF3 is low (all three)
        # Check if all three Cavity_SRF channels are low in FIRST
        cavity_low_channels_all = []
        cavity_specific_channels = []

        # Check for specific channels: Cavity_SRF1_v, Cavity_SRF2_v, Cavity_SRF3_v
        target_channels = ["Cavity_SRF1_v", "Cavity_SRF2_v", "Cavity_SRF3_v"]
        for channel in target_channels:
            if has_in_group(first, channel, "low"):
                cavity_low_channels_all.append(channel)
                # Clean channel name for display
                clean_name = channel.replace("_v", "")
                cavity_specific_channels.append(clean_name)

        # Also check BeamCurrent is lowlow in THIRD (should always be there)
        beam_lowlow_in_third = has_in_group(third, "BeamCurrent_v", "lowlow")

        if len(cavity_low_channels_all) == 3 and beam_lowlow_in_third:
            channel_names = ", ".join(cavity_specific_channels)
            return {
                "case": 13,
                "description": f"{channel_names} are low",
                "fault": f"'{channel_names}' are low → All RF station moved together. Check common RF source like master oscillator,..",
                "confidence": 0.80
            }

        # Rule 11: Cavity_SRF# is low (general case - one or more)
        cavity_low_channels = get_channels_with_event(first, "Cavity_SRF", "low")
        if cavity_low_channels:
            channel_names = format_channel_names(cavity_low_channels)
            fault_msg = format_fault_with_numbers("RF station# moved first. Check RF station# path", cavity_low_channels)

            if len(cavity_low_channels) == 1:
                description = f"{channel_names} is low"
            else:
                description = f"{channel_names} are low"

            return {
                "case": 11,
                "description": description,
                "fault": f"'{channel_names}' is low → {fault_msg}",
                "confidence": 0.75
            }

        # Rule 12: Cavity_SRF# is high
        cavity_high_channels = get_channels_with_event(first, "Cavity_SRF", "highhigh")
        if cavity_high_channels:
            channel_names = format_channel_names(cavity_high_channels)
            fault_msg = format_fault_with_numbers("The Cavity# was detuned. Check RF path of Cavity#.", cavity_high_channels)

            if len(cavity_high_channels) == 1:
                description = f"{channel_names} is high"
            else:
                description = f"{channel_names} are high"

            return {
                "case": 12,
                "description": description,
                "fault": f"'{channel_names}' is high → {fault_msg}",
                "confidence": 0.70
            }

        # No rule matched
        return {
            "case": 0,
            "description": "No matching pattern found",
            "fault": "Unknown fault pattern",
            "confidence": 0.0
        }
