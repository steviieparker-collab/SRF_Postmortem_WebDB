"""Data structures for standalone SRF event classification, ported from the monitor system."""

from dataclasses import dataclass, field
from typing import List, Tuple


# Digital delay compensation constant (ms)
DIGITAL_DELAY_MS = 3.0


@dataclass
class SignalEvent:
    """Represents a detected signal variation event."""
    time: float           # absolute time in seconds
    channel: str          # channel name
    event_type: str       # "low", "lowlow", "highhigh", "digital"
    value: float          # signal value at event time

    @property
    def effective_time(self) -> float:
        """Time used for ordering - compensated for digital delay."""
        if self.event_type == "digital":
            return self.time - DIGITAL_DELAY_MS / 1000.0
        return self.time

    def __repr__(self):
        return f"SignalEvent({self.channel}, t={self.time*1000:.2f}ms, {self.event_type})"

    def __lt__(self, other):
        return self.effective_time < other.effective_time


@dataclass
class TimeGroup:
    """Represents FIRST, SECOND, THIRD time groups."""
    name: str
    start_time: float = 0.0
    events: List[SignalEvent] = field(default_factory=list)

    def add_event(self, event: SignalEvent) -> None:
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
