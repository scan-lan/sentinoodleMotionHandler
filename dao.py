import pymysql
from pymysql.err import OperationalError
from schemas import Session
from datetime import datetime
from typing import Optional, List

pymysql_config = {
    'user': 'cloudFunction',
    'password': '$D>rj>!rr4?w6=V^Sa^z',
    'db': 'sentinoodlev2',
    'charset': 'utf8mb4',
    'cursorclass': pymysql.cursors.DictCursor,
    'autocommit': True
}

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


def insert_event_into_table(id: str, session_id: int, event_name: str, published_at: str, room: str) -> None:
    event_insert_query = f"""
        INSERT INTO event (id, session_id, event_name, published_at, room)
        VALUES (
            '{id}',
            {session_id},
            '{event_name}',
            STR_TO_DATE('{published_at[:-1]}000', '%Y-%m-%dT%H:%i:%s.%f'),
            '{room}');
    """
    ensure_db_connection()

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
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(fetch_session_query)

    session_record = cursor.fetchone()
    return Session(**session_record)


def get_last_action_time(session_id: int) -> Optional[datetime]:
    fetch_last_action = f"""
        SELECT action_taken
        FROM `action`
            JOIN event e ON triggering_event_id = e.id
        WHERE session_id = {session_id}
        ORDER BY published_at DESC
        LIMIT 1;
    """
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(fetch_last_action)

    last_action_time = cursor.fetchone()["action_taken"]
    if not last_action_time:
        return None
    return last_action_time


def get_messages(session_id: int) -> List[str]:
    get_messages_query = f"""
        SELECT message_text
        FROM message
        WHERE session_id = {session_id};
    """
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(get_messages_query)

    messages = [message["message_text"] for message in cursor.fetchall()]
    return messages


def update_message_index(session_id: int, new_index_value: int) -> None:
    update_index_query = f"""
        UPDATE session
        SET message_index = {new_index_value}
        WHERE id = {session_id};
    """
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(update_index_query)


def insert_action_into_table(triggering_event_id: str, action_type: str, body: str):
    action_insert_query = f"""
        INSERT INTO `action` (triggering_event_id, `type`, body, action_taken)
        VALUES (
            '{triggering_event_id}',
            '{action_type}',
            '{body}',
            NOW());
    """
    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(action_insert_query)
