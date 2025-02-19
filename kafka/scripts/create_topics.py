import json
import os
import time
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable
from dotenv import load_dotenv

load_dotenv()
boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS')


def create_topics_from_config(config_file, bootstrap_servers):
    with open(config_file, 'r') as f:
        config = json.load(f)
    topics = config.get('topics', [])
    new_topics = []
    for topic in topics:
        topic_name = topic.get('name')
        num_partitions = topic.get('partitions', 1)
        replication_factor = topic.get('replication_factor', 1)
        new_topics.append(
            NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))

    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='topic_creator')
    admin_client.create_topics(new_topics=new_topics, validate_only=False)
    print("Topics created successfully.")


def create_topics_with_retry(config_file, bootstrap_servers, retries=5, delay=5):
    for attempt in range(retries):
        try:
            create_topics_from_config(config_file, bootstrap_servers)
            return
        except NoBrokersAvailable as e:
            print(f"No brokers available, retrying in {delay} seconds (attempt {attempt + 1} of {retries})")
            time.sleep(delay)
        except Exception as e:
            print(f"Error creating topics: {e}")
            break
    print("Failed to create topics after retries.")


if __name__ == "__main__":
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', boot)
    create_topics_with_retry('topics_config.json', bootstrap_servers)
