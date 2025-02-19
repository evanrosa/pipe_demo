import json
import os
from kafka.admin import KafkaAdminClient, NewTopic

def create_topics_from_config(config_file, bootstrap_servers):
    with open(config_file, 'r') as f:
        config = json.load(f)
    topics = config.get('topics', [])
    new_topics = []
    for topic in topics:
        topic_name = topic.get('name')
        num_partitions = topic.get('partitions', 1)
        replication_factor = topic.get('replication_factor', 1)
        new_topics.append(NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor))
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id='topic_creator')
    try:
        admin_client.create_topics(new_topics=new_topics, validate_only=False)
        print("Topics created successfully.")
    except Exception as e:
        print(f"Error creating topics: {e}")

if __name__ == "__main__":
    bootstrap_servers = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    create_topics_from_config('topics_config.json', bootstrap_servers)