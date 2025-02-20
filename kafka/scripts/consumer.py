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
    client = bigquery.Client(project=os.getenv("BIGQUERY_PROJECT"))
    return client

def ensure_table_exists(client, table_id):
    project = os.getenv("BIGQUERY_PROJECT")
    dataset_id = os.getenv("BIGQUERY_DATASET")
    table_ref = client.dataset(dataset_id).table(table_id)
    try:
        client.get_table(table_ref)
        print("Table already exists.")
    except Exception as e:
        # Define schema matching your events
        schema = [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("subscription_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {project}.{dataset_id}.{table_id}")

def insert_event(client, event, table_id):
    project = os.getenv("BIGQUERY_PROJECT")
    dataset_id = os.getenv("BIGQUERY_DATASET")
    table_ref = client.dataset(dataset_id).table(table_id)
    # Insert the event as a single row
    rows_to_insert = [event]
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if not errors:
        print(f"Event inserted: {event}")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def process_topic(topic, table):
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', boot)
    consumer = create_consumer(bootstrap_servers, topic)
    client = connect_to_bq()
    ensure_table_exists(client, table)
    print(f"Started consuming events from topic '{topic}' into table '{table}'...")
    for msg in consumer:
        event = msg.value
        insert_event(client, table, event)

def main():
    topics = [
        "subscriptions",
        "revenue",
        "churn",
        "customer_engagement"
    ]
    tables = [
        "raw_subscription_events",
        "raw_revenue_events",
        "raw_churn_events",
        "raw_customer_engagement_events"
    ]

    threads = []
    for topic, table in zip(topics, tables):
        t = threading.Thread(target=process_topic, args=(topic, table))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

if __name__ == "__main__":
    main()
