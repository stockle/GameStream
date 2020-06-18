import json
import event_consumer
from database import cassandra_connector

def handle_purchase_event(db, event):
    query = """
        INSERT INTO purchase_events
        (user_id, event_time, game, platform, item, price)
        VALUES (%s,%s,%s,%s,%s,%s)
    """
    db.insert(query, [(
        event['UID'], event['Time'][:-3],
        event['event_body']['Game'],
        event['event_body']['Platform'],
        event['event_body']['Item'],
        event['event_body']['Price']
    )])

def create_purchase_events(db):
    query = """DROP TABLE IF EXISTS purchase_events"""
    db.execute(query)

    query = """
        CREATE TABLE IF NOT EXISTS purchase_events
        (
            PRIMARY KEY(user_id, event_time, item),
            user_id text, event_time timestamp,
            game text, platform text, item text,
            price decimal
        );
    """
    db.execute(query)

def init_db(db):
    db.init_cluster()
    db.init_session()
    db.init_keyspace('v1')

if __name__=="__main__":
    db = cassandra_connector.DBConnector()
    init_db(db)
    create_purchase_events(db)

    purchase_evcon = event_consumer.EventConsumer(db,
        handle_purchase_event,
        'purchase_event'
    )

    purchase_evcon.consume()