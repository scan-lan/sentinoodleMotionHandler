import base64
from datetime import datetime, timedelta
import json
from dataclasses import asdict
from random import Random
from typing import Optional

from requests import post
from schemas import Session, Event, Action
from dao import (
    get_session_info, insert_event_into_table, get_last_action_time,
    update_message_index, insert_action_into_table, get_messages
)

MESSAGE_SEPARATOR = '|'
BEDROOM = "bedroom"
KITCHEN = "kitchen"
GIVE_AFFIRMATION = "give_affirmation"
MEDICATION_REMINDER = "medication_reminder"


def extract_event_fields(event, event_id: str) -> dict[str, str, str, str]:
    decoded_event_data = base64.b64decode(event["data"]).decode("utf-8")
    data_dict = json.loads(decoded_event_data)

    event_name = event["attributes"]["event"]
    published_at = event["attributes"]["published_at"]
    room = data_dict["room"]
    return {"id": event_id, "event_name": event_name, "published_at": published_at, "room": room}


def should_send_message(session_id: int, message_wait_time: int) -> bool:
    last_action_time = get_last_action_time(session_id)
    if not last_action_time:
        return True
    now_minus_wait_time = datetime.now() - timedelta(minutes=message_wait_time)
    return now_minus_wait_time > last_action_time


def determine_action(event: Event, session: Session) -> Optional[str]:
    if should_send_message(session.id, session.message_wait_period_minutes):
        if event.room == BEDROOM:
            return GIVE_AFFIRMATION
        elif event.room == KITCHEN:
            return MEDICATION_REMINDER
    return None


def get_message_to_use(session: Session):
    messages = get_messages(session.id)
    message_index = 0 if not session.message_index or session.message_index >= len(messages) else session.message_index
    message_to_use = messages[message_index]
    new_message_index = 0 if message_index + 1 >= len(messages) else message_index + 1
    update_message_index(session.id, new_message_index)
    return message_to_use


def handle_motion(event, context) -> None:
    event_fields = extract_event_fields(event, context.event_id)
    device_id = event["attributes"]["device_id"]

    session = get_session_info(device_id)
    event_record = Event(**event_fields, session_id=session.id)
    insert_event_into_table(**asdict(event_record))

    action_type = determine_action(event_record, session)
    if action_type:
        message = get_message_to_use(session)
        url = f"https://maker.ifttt.com/trigger/{action_type}/with/key/pCu9XeUesIiztDzTb4jCaXdd-j_c69EgNA0Ms4WB4vZ"
        result = post(url)
        print(f"Status: {result.status_code}; Text: {result.text}")
        action_record = Action(event_record.id, action_type, f'{{\"message\": \"{message}\"}}')
        insert_action_into_table(**asdict(action_record))
