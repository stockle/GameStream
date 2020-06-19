import sys
import json
import logging
from datetime import datetime
from kafka import KafkaConsumer

class EventConsumer:
    def __init__(self, db, handler, topic='topic', group=None):
        self.db = db
        self.group = group
        self.topic = topic
        self.handler = handler

    def consume(self):
        bootstrap_servers = ['localhost:9092']
        consumer = KafkaConsumer(
            self.topic,
            group_id=self.group,
            enable_auto_commit=True,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='earliest',
            value_deserializer=lambda m: json.loads(m.decode('ascii'))
        )
        logging.basicConfig(
            filename='/tmp/gamestream-main-'
            + datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")
            + '-.log',
            level=logging.DEBUG
        )
        try:
            consumer.poll()
            for message in consumer:
                event = message.value
                print(event)
                logging.info(
                    'Received message ('
                    + self.topic + '/'
                    + '):', event
                )
                self.handler(self.db, event)
        except KeyboardInterrupt:
            sys.exit()

if __name__ == "__main__":
    pass
