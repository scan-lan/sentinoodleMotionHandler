import base64
import json
from dataclasses import asdict
from requests import post
from schemas import Session, Event
from dao import get_session_info, insert_event_into_table

MESSAGE_SEPARATOR = '|'  # TODO: implement this


def extract_event_fields(event, event_id: str) -> dict[str, str, str, str]:
    decoded_event_data = base64.b64decode(event["data"]).decode("utf-8")
    data_dict = json.loads(decoded_event_data)
    print(data_dict)

    event_name = event["attributes"]["event"]
    published_at = event["attributes"]["published_at"]
    room = data_dict["room"]
    return {"id": event_id, "event_name": event_name, "published_at": published_at, "room": room}


def handle_motion(event, context) -> None:
    print(event)
    event_fields = extract_event_fields(event, context.event_id)
    device_id = event["attributes"]["device_id"]
    session = get_session_info(device_id)
    event_record = Event(session_id=session.id, **event_fields)
    insert_event_into_table(**asdict(event_record))

    # if action != "none":
    #     url = f"https://maker.ifttt.com/trigger/{action}/with/key/pCu9XeUesIiztDzTb4jCaXdd-j_c69EgNA0Ms4WB4vZ"
    #     result = post(url)
    #     print(f"Status: {result.status_code}; Text: {result.text}")
