import os
import json
import time
import datetime
import random
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
from kafka.errors import NoBrokersAvailable

fake = Faker()
load_dotenv()

boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

def generate_event():
    return {
        "user_id": fake.uuid4(),
        "event_time": datetime.datetime.now().isoformat(),
        "subscription_amount": round(random.uniform(10, 100), 2),
        "event_type": random.choice(["new_subscription", "cancellation", "renewal"])
    }

def create_producer(bootstrap_servers, retries=5):
    for attempt in range(retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=bootstrap_servers,
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
            return producer
        except NoBrokersAvailable:
            print(f"No brokers available, retrying in 5 seconds (attempt {attempt + 1})")
            time.sleep(5)
    raise Exception("Kafka broker is not available after retries")

def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', boot)
    topic = os.environ.get('KAFKA_TOPIC', 'subscriptions')

    producer = create_producer(bootstrap_servers)

    while True:
        event = generate_event()
        producer.send(topic, event)
        print(f"Event sent: {event}")
        time.sleep(1)

if __name__ == "__main__":
    main()