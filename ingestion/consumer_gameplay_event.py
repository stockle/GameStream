import json
import EventConsumer
from database import cassandra_connector

def handle_gameplay_event(db, event):
    query = """
        INSERT INTO gameplay_events
        (user_id, event_time, game, platform, platform_stats)
        VALUES (%s,%s,%s,%s,%s)
    """
    db.insert(query, [(
        event['UID'], event['Time'][:-3],
        event['event_body']['Game'],
        event['event_body']['Platform'],
        json.dumps(event['event_body']['PlatformStats'])
    )])

def create_gameplay_events(db):
    query = """DROP TABLE IF EXISTS gameplay_events"""
    db.execute(query)

    query = """
        CREATE TABLE IF NOT EXISTS gameplay_events
        (
            PRIMARY KEY(user_id, event_time),
            user_id text, event_time timestamp,
            game text, platform text,
            platform_stats text
        );
    """
    db.execute(query)

def init_db(db):
    db.init_cluster()
    db.init_session()
    db.init_keyspace('v1')

if __name__=="__main__":
    db = cassandra_connector.DBConnector()
    create_gameplay_events(db)

    gameplay_evcon = event_consumer.EventConsumer(db,
        handle_gameplay_event,
        'gameplay_event',
        'gameplay_event'
    )

    gameplay_evcon.consume()