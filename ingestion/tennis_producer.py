import json
from kafka import KafkaProducer
from kafka.errors import KafkaError
import time

class MatchEventProducer:
    def __init__(self, bootstrap_servers="kafkaServer:9092", events=dict):
        self.events = events
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda m: json.dumps(m).encode('utf-8'),
            retries=5
        )

    def produce_events(self, topic='new_point'):
        for event in self.events:
            future = self.producer.send(topic, event)
            try:
                record_metadata = future.get(timeout=10)
                print(f"Event sent to topic {record_metadata.topic}, partition {record_metadata.partition}, offset {record_metadata.offset}")
                time.sleep(1)
            except KafkaError as e:
                print(f"Failed to send event: {e}")

        self.producer.flush()
        
    def close(self):
        self.producer.close()

if __name__ == "__main__":
    with open('tennis_match_events.json', 'r') as file:
        events = json.load(file)
    producer = MatchEventProducer(events=events)
    producer.produce_events()
    producer.close()