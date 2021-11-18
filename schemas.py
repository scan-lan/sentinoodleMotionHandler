from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from decimal import Decimal


@dataclass
class Session:
    id: int
    device_id: str
    datetime_started: datetime
    medication_name: Optional[str]
    medication_dosage: Optional[Decimal]
    dosage_frequency: Optional[int]
    messages: Optional[str]
    message_index: Optional[int]
    message_wait_period_minutes: Optional[int]
    doses_taken_today: Optional[int]


@dataclass
class Event:
    id: str
    session_id: int
    event_name: str
    published_at: str
    room: Optional[str]


@dataclass
class Action:
    triggering_event_id: str
    action_type: str
    body: str
