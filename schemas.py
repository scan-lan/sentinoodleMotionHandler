from dataclasses import dataclass
from typing import List, Optional
from datetime import datetime
from decimal import Decimal


@dataclass
class Session:
    id: int
    device_id: str
    medication_id: int
    datetime_started: datetime
    message_index: Optional[int]
    message_wait_period_minutes: Optional[int]


@dataclass
class Event:
    id: str
    session_id: int
    event_name: str
    published_at: str
    room: Optional[str]


@dataclass
class Action:
    id: int
    triggering_event_id: str
    action_type: str
    body: str
    action_taken: datetime
