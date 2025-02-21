import os
import json
import threading
import time
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
from google.cloud import bigquery
from dotenv import load_dotenv
from google.api_core.exceptions import NotFound

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
    table_ref = bigquery.TableReference.from_string(f"{project}.{dataset_id}.{table_id}")

    try:
        # Try to retrieve the table. If it exists, simply return.
        client.get_table(table_ref)
        print(f"Table {table_id} already exists.")
        return
    except NotFound:
        # Table not found. Proceed to create it.
        print(f"Table {table_id} does not exist. Creating table...")

    # Define schemas for known tables.
    schemas = {
        "raw_subscription_events": [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("subscription_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subscription_plan", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("signup_source", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("trial", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_segment", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("contract_length_months", "INTEGER", mode="REQUIRED"),
        ],
        "raw_revenue_events": [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("payment_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("payment_method", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("discount_applied", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("invoice_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("recurring_period", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("due_date", "TIMESTAMP", mode="REQUIRED"),
        ],
        "raw_churn_events": [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("churn_risk", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("customer_tenure_days", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("last_activity_gap_days", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("support_ticket_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("usage_drop_percentage", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("satisfaction_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("cancellation_reason", "STRING", mode="NULLABLE"),
        ],
        "raw_customer_engagement_events": [
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("engagement_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("session_duration", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("login_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("dashboard_views", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("financing_inquiries", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("feature_interaction_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("api_request_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("referral_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("capital_access_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("report_download_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("integration_usage", "INTEGER", mode="REQUIRED"),
        ]
    }

    schema = schemas.get(table_id)
    if not schema:
        print(f"Table schema not found for table {table_id}")
        return

    try:
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"Created table {project}.{dataset_id}.{table_id}")
    except Exception as e:
        # Log any unexpected errors during table creation.
        print(f"Error creating table {table_id}: {e}")



def insert_event(client, event, table_id):
    project = os.getenv("BIGQUERY_PROJECT")
    dataset_id = os.getenv("BIGQUERY_DATASET")
    table_ref = f"{project}.{dataset_id}.{table_id}"

    # Wrap the event data correctly for BigQuery
    rows_to_insert = [event]
    print(
        f"Inserting event into table {table_ref} with data: {rows_to_insert}"
    )
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if not errors:
        print(f"Event inserted: {event}")
    else:
        print("Encountered errors while inserting rows: {}".format(errors))

def process_topic(consumer, topic, table):
    client = connect_to_bq()
    ensure_table_exists(client, table)
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
    client = connect_to_bq()

    threads = []
    for topic, table_id in topics_to_tables.items():
        consumer = create_consumer(bootstrap_servers, topic)
        ensure_table_exists(client, table_id)

        thread = threading.Thread(target=process_topic, args=(consumer, topic, table_id))
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
