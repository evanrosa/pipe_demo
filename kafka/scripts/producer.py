import os
import json
import time
import random
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
from kafka.errors import NoBrokersAvailable

fake = Faker()
load_dotenv()

boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS')


# Generate a random date over the past 5 years
def random_event_time():
    return fake.date_time_between(start_date="-5y", end_date="now").isoformat()


def generate_subscription():
    return {
        "user_id": fake.uuid4(),
        "event_time": random_event_time(),
        "subscription_amount": round(random.uniform(10, 100), 2),
        "event_type": random.choice(["new_subscription", "cancellation", "renewal"])
    }


def generate_revenue():
    return {
        "user_id": fake.uuid4(),
        "event_time": random_event_time(),
        "payment_amount": round(random.uniform(10, 200), 2),
        "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"])
    }


def generate_churn():
    return {
        "user_id": fake.uuid4(),
        "event_time": random_event_time(),
        "churn_risk": round(random.uniform(0, 1), 2)
    }


def generate_customer_engagement():
    return {
        "user_id": fake.uuid4(),
        "event_time": random_event_time(),
        "engagement_score": random.randint(1, 100)
    }


# Map topics to event generator functions
TOPIC_GENERATORS = {
    "subscriptions": generate_subscription,
    "revenue": generate_revenue,
    "churn": generate_churn,
    "customer_engagement": generate_customer_engagement
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
    # Get topics from env variable; default is all topics
    topic_list = os.environ.get('KAFKA_TOPICS', 'subscriptions,revenue,churn,customer_engagement').split(',')

    producer = create_producer(bootstrap_servers)

    while True:
        for topic in topic_list:
            generator = TOPIC_GENERATORS.get(topic.strip(), generate_subscription)
            event = generator()
            producer.send(topic, event)
            print(f"Event sent to {topic}: {event}")
        time.sleep(1)


if __name__ == "__main__":
    main()
