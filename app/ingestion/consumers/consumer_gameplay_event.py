import json

def handle_gameplay_event(db, event):
    query = """
        INSERT INTO gameplay_events
        (user_id, event_time, game, platform, platform_stats)
        VALUES (%s,%s,%s,%s,%s)
    """
    db.insert(query, [(
        event['UID'], event['Time'],
        event['event_body']['Game'],
        event['event_body']['Platform'],
        json.dumps(event['event_body']['PlatformStats'])
    )])