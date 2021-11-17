import pymysql
from pymysql.err import OperationalError
from schemas import Session

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
        """
    event_insert_query += f"'{room}');" if room else "NULL);"
    print(event_insert_query)

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
    print(fetch_session_query)

    ensure_db_connection()

    with __get_cursor() as cursor:
        cursor.execute(fetch_session_query)

    session_record = cursor.fetchone()
    print(session_record)

    return Session(**session_record)