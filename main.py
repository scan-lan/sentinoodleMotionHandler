import base64
import json
import logging as LOG
from typing import NewType

import pymysql
from pymysql.err import OperationalError
from requests import post

from schemas import Session

MESSAGE_SEPARATOR = '|'  # TODO: implement this

pymysql_config = {
    'user': 'cloudFunction',
    'password': '$D>rj>!rr4?w6=V^Sa^z',
    'db': 'sentinoodlev2',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}

# isession = NewType("Session", Session)

CONNECTION_NAME = 'sentinoodle:europe-west2:sentinoodle-events'

# Create SQL connection globally to enable reuse
# PyMySQL does not include support for connection pooling
DB = None


def __get_cursor():
    """
    Helper function to get a cursor.
    PyMySQL does NOT automatically reconnect, so we must reconnect explicitly using ping().
    """
    try:
        return DB.cursor()
    except OperationalError:
        DB.ping(reconnect=True)
        return DB.cursor()


def ensure_db_connection():
    global DB

    if not DB:
        try:
            DB = pymysql.connect(**pymysql_config)
        except OperationalError:
            # If production settings fail, use local development ones
            pymysql_config['unix_socket'] = f'/cloudsql/{CONNECTION_NAME}'
            DB = pymysql.connect(**pymysql_config)


# TODO: remove device_id from method signature
def insert_event_into_table(session_id: int, event_id: str, event_name: str, date_string: str, room: str) -> None:
    event_insert_query = f"""
            INSERT INTO event (gc_pub_sub_id, session_id, event, published_at, room)
            VALUES (
                '{event_id}',
                {session_id},
                '{event_name}',
                STR_TO_DATE('{date_string[:-1]}000', '%Y-%m-%dT%H:%i:%s.%f'),
        """
    event_insert_query += f"'{room}');" if room else "NULL);"
    LOG.info(event_insert_query)

    ensure_db_connection()

    # Remember to close SQL resources declared while running this function
    with __get_cursor() as cursor:
        cursor.execute(event_insert_query)


def get_session_info(device_id: str) -> Session:
    fetch_session_query = f"""
        SELECT *
            FROM session
        WHERE device_id = '{device_id}'
        ORDER BY datetime_started DESC
        LIMIT 1;
    """
    LOG.info(fetch_session_query)

    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(fetch_session_query)

    session_record = cursor.fetchone()
    LOG.info(session_record)

    return Session(**session_record)


def extract_event_fields(event, context) -> dict[str, str, str, str]:
    decoded_event_data = base64.b64decode(event["data"]).decode("utf-8")
    data_dict = json.loads(decoded_event_data)
    LOG.info(data_dict)

    event_id = context.event_id
    event_name = event["attributes"]["event"]
    date_string = event["attributes"]["published_at"]
    room = data_dict["room"]
    return {"event_id": event_id, "event_name": event_name, "date_string": date_string, "room": room}


def handle_motion(event, context) -> None:
    event_fields = extract_event_fields(event, context)
    device_id = event["attributes"]["device_id"]
    session_record = get_session_info(device_id)
    insert_event_into_table(session_record.id, *event_fields.values())

    # if action != "none":
    #     url = f"https://maker.ifttt.com/trigger/{action}/with/key/pCu9XeUesIiztDzTb4jCaXdd-j_c69EgNA0Ms4WB4vZ"
    #     result = post(url)
    #     print(f"Status: {result.status_code}; Text: {result.text}")
