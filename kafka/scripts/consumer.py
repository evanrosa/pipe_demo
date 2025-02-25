import os
import json
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()
boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS')
project = os.getenv("BIGQUERY_PROJECT")
dataset_id = os.getenv("BIGQUERY_DATASET")

def create_consumer(bootstrap_servers, topic, retries=5):
    for attempt in range(retries):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=bootstrap_servers,
                auto_offset_reset='earliest',
                enable_auto_commit=False,
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            return consumer
        except NoBrokersAvailable:
            print(f"No brokers available, retrying in 5 seconds (attempt {attempt + 1})")
            time.sleep(5)
    raise Exception("Kafka broker is not available after retries")

def connect_to_bq():
    # BigQuery client; credentials are picked up from GOOGLE_APPLICATION_CREDENTIALS env var
    return bigquery.Client(project=project)

def insert_event(client, event, table_id, max_retries=3):
    table_ref = f"{project}.{dataset_id}.{table_id}"
    rows_to_insert = [event]
    for attempt in range(max_retries):
        try:
            errors = client.insert_rows_json(table_ref, rows_to_insert)
            if not errors:
                print(f"Event inserted: {event}")
                return
            else:
                print(f"Encountered errors: {errors}")
                break  # Non-retryable errors, so exit loop
        except Exception as e:
            print(f"Unexpected error: {e}")
            break
        time.sleep(1)
    print(f"Failed to insert event after {max_retries} attempts: {event}")

def process_topic(consumer, topic, table):
    client = connect_to_bq()
    print(f"Started consuming events from topic '{topic}' into table '{table}'...")
    for msg in consumer:
        event = msg.value
        insert_event(client, event, table)

def main():
    topics_to_tables = {
        "subscriptions": "raw_subscription_events",
        "revenue": "raw_revenue_events",
        "churn": "raw_churn_events",
        "customer_engagement": "raw_customer_engagement_events"
    }
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', boot)
    threads = []
    for topic, table_id in topics_to_tables.items():
        consumer = create_consumer(bootstrap_servers, topic)
        thread = threading.Thread(target=process_topic, args=(consumer, topic, table_id))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
