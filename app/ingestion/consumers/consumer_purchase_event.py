import json

def handle_purchase_event(db, event):
    query = """
        INSERT INTO purchase_events
        (user_id, event_time, game, platform, purchase_info)
        VALUES (%s,%s,%s,%s,%s)
    """
    db.insert(query, [(
        event['UID'], event['Time'],
        event['event_body']['Game'],
        event['event_body']['Platform'],
        json.dumps(event['event_body']['PurchaseInfo'])
    )])