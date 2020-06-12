import json

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