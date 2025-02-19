import os
import json
import time
import datetime
import random
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

def generate_event():
    return {
        "user_id": fake.uuid4(),
        "event_time": datetime.datetime.now().isoformat(),
        "subscription_amount": round(random.uniform(10, 100), 2),
        "event_type": random.choice(["new_subscription", "cancellation", "renewal"])
    }

def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.environ.get('KAFKA_TOPIC', 'subscriptions')

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

    while True:
        event = generate_event()
        producer.send(topic, event)
        print(f"Event sent: {event}")
        time.sleep(1)

if __name__ == "__main__":
    main()