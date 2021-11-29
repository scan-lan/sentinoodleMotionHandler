import base64
from datetime import datetime, timedelta
import json
from dataclasses import asdict
from typing import Optional

from requests import post
from schemas import Session, Event
from dao import (
    get_session_info, insert_event_into_table, get_last_action,
    update_message_index, insert_action_into_table, get_messages,
    get_events_today
)

BEDROOM = "bedroom"
KITCHEN = "kitchen"
GIVE_AFFIRMATION = "give_affirmation"
MEDICATION_REMINDER = "medication_reminder"
EAT_REMINDER = "eat_reminder"
EATEN = "eaten"


def extract_event_fields(event, event_id: str) -> dict[str, str, str, str]:
    decoded_event_data = base64.b64decode(event["data"]).decode("utf-8")
    data_dict = json.loads(decoded_event_data)

    event_name = event["attributes"]["event"]
    published_at = event["attributes"]["published_at"]
    room = data_dict["room"]
    return {"id": event_id, "event_name": event_name, "published_at": published_at, "room": room}


def should_send_message(session_id: int, message_wait_time: int) -> bool:
    try:
        last_action_time = get_last_action(session_id).action_taken
    except AttributeError:
        return True
    else:
        now_minus_wait_time = datetime.now() - timedelta(minutes=message_wait_time)
        return now_minus_wait_time > last_action_time


def have_eaten_today(session_id: int):
    return EATEN in [event.event_name for event in get_events_today(session_id)]


def determine_action(event: Event, session: Session) -> Optional[str]:
    if should_send_message(session.id, session.message_wait_period_minutes):
        if event.room == BEDROOM:
            return GIVE_AFFIRMATION
        elif event.room == KITCHEN:
            if not have_eaten_today(session.id):
                return EAT_REMINDER
            else:
                return MEDICATION_REMINDER
    return None


def get_message_to_use(session: Session):
    messages = get_messages(session.id)
    message_index = 0 if not session.message_index or session.message_index >= len(messages) else session.message_index
    message_dict = {"index": message_index, "message": messages[message_index]}
    new_message_index = 0 if message_index + 1 >= len(messages) else message_index + 1
    update_message_index(session.id, new_message_index)
    return message_dict


def perform_action(action_type: str, message_dict=None):
    url_trigger_name = f"{GIVE_AFFIRMATION}{message_dict['index']}" if action_type == GIVE_AFFIRMATION else action_type
    url = f"https://maker.ifttt.com/trigger/{url_trigger_name}/with/key/pCu9XeUesIiztDzTb4jCaXdd-j_c69EgNA0Ms4WB4vZ"
    result = post(url)
    print(f"Status: {result.status_code}; Text: {result.text}")


def handle_motion(event, context) -> None:
    event_fields = extract_event_fields(event, context.event_id)
    device_id = event["attributes"]["device_id"]

    session = get_session_info(device_id)
    event_record = Event(**event_fields, session_id=session.id)
    insert_event_into_table(**asdict(event_record))

    action_type = determine_action(event_record, session)
    if action_type:
        if action_type != GIVE_AFFIRMATION:
            perform_action(action_type)
            action_record = {"triggering_event_id": event_record.id,
                             "action_type": action_type,
                             "body": action_type}
            insert_action_into_table(**action_record)
        else:
            message_dict = get_message_to_use(session)
            perform_action(action_type, message_dict)
            action_record = {"triggering_event_id": event_record.id,
                             "action_type": action_type,
                             "body": message_dict["message"]}
            insert_action_into_table(**action_record)
