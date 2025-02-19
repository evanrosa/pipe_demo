import os
import json
import psycopg2
from kafka import KafkaConsumer

def connect_to_db():
    host = os.environ.get('POSTGRES_HOST', 'postgres')
    port = os.environ.get('POSTGRES_PORT', 5432)
    user = os.environ.get('POSTGRES_USER', 'postgres')
    password = os.environ.get('POSTGRES_PASSWORD', '<PASSWORD>')
    db = os.environ.get('POSTGRES_DB', 'pipe_demo')

    conn = psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=db
    )
    conn.autocommit = True
    return conn

def create_table_if_not_exists(conn):
    create_table_query = """
        CREATE TABLE IF NOT EXISTS raw_subscription_events (
            id SERIAL PRIMARY KEY,
            user_id VARCHAR(255) NOT NULL,
            event_time TIMESTAMP NOT NULL,
            subscription_amount DECIMAL(10, 2) NOT NULL,
            event_type TEXT NOT NULL
        );
    """
    with conn.cursor() as cursor:
        cursor.execute(create_table_query)
    print("Ensured raw_subscription_events table exists.")

def insert_event(conn, event):
    insert_query = """
        INSERT INTO raw_subscription_events (user_id, event_time, subscription_amount, event_type)
        VALUES (%s, %s, %s, %s);
    """
    with conn.cursor() as cursor:
        cursor.execute(insert_query, (
            event.get('user_id'),
            event.get('event_time'),
            event.get('subscription_amount'),
            event.get('event_type')
        ))
    print(f"Event inserted: {event}")

def main():
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    topic = os.environ.get('KAFKA_TOPIC', 'subscriptions')

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset='earliest',
        enable_auto_commit=False,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

    conn = connect_to_db()
    create_table_if_not_exists(conn)

    print("Starting to consume events...")
    for msg in consumer:
        event = msg.value
        insert_event(conn, event)

if __name__ == "__main__":
    main()