import os
import json
import time
import random
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
from kafka.errors import NoBrokersAvailable
import datetime

fake = Faker()
load_dotenv()

boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Generate a fixed list of user IDs (e.g., 100 users)
NUM_USERS = 1000
FIXED_USER_IDS = [str(fake.uuid4()) for _ in range(NUM_USERS)]

# Generate a random date over the past 5 years
def random_event_time():
    return fake.date_time_between(start_date="-5y", end_date="now").isoformat()

def generate_subscription():
    return {
        "user_id": random.choice(FIXED_USER_IDS),  # Unique identifier for the user
        "event_time": random_event_time(),  # Timestamp for the event, spanning the past 5 years
        "subscription_amount": round(random.uniform(1000, 10000), 2),  # Monthly recurring revenue amount
        "event_type": random.choice(["new_subscription", "cancellation", "renewal"]),  # Type of event
        "subscription_plan": random.choice(["basic", "pro", "enterprise"]),  # Tier of subscription
        "signup_source": random.choice(["website", "mobile_app", "partner_referral"]),  # How the user signed up
        "trial": random.choice([True, False]),  # Whether the subscription is a trial subscription
        "region": random.choice(["North America", "Europe", "Asia", "Other"]),  # Geographic region of the user
        "user_segment": random.choice(["SMB", "Enterprise", "Startup"]),  # Customer segment classification
        "contract_length_months": random.choice([12, 24, 36, 48, 60])  # Length of the customer contract in months
    }

def generate_revenue():
    event_time_str = random_event_time()
    start_dt = datetime.datetime.fromisoformat(event_time_str)

    # Revenue events simulate actual payment transactions
    return {
        "user_id": random.choice(FIXED_USER_IDS),                             # Unique user identifier
        "event_time": event_time_str,                             # Timestamp of the revenue event
        "payment_amount": round(random.uniform(1000, 10000), 2),  # Amount paid in the transaction
        "payment_method": random.choice(["credit_card", "paypal", "bank_transfer"]),  # Payment method used
        "discount_applied": round(random.uniform(0, 20), 2),  # Any discount applied to the payment
        "currency": random.choice(["USD", "EUR", "GBP"]),     # Currency used for the payment
        "invoice_id": fake.uuid4(),                           # A simulated invoice ID for the transaction
        "recurring_period": random.choice(["monthly", "quarterly", "annual"]),  # Payment frequency
        "due_date": (fake.date_time_between(start_date=start_dt, end_date="+30d")).isoformat()  # Simulated due date
    }

def generate_churn():
    # Churn events can capture indicators that a customer is at risk
    churn_event = {
        "user_id": random.choice(FIXED_USER_IDS),                         # Unique identifier for the user
        "event_time": random_event_time(),                          # Timestamp when the churn event is recorded
        "churn_risk": round(random.uniform(0, 1), 2),     # Churn probability (0 to 1)
        "customer_tenure_days": random.randint(30, 1000),  # Days the user has been a customer
        "last_activity_gap_days": random.randint(1, 90),   # Days since the user's last activity
        "support_ticket_count": random.randint(0, 5),      # Number of support tickets filed recently
        "usage_drop_percentage": round(random.uniform(0, 50), 2),  # Drop in user activity compared to a previous period
        "satisfaction_score": random.randint(1, 10)        # Simulated satisfaction score (e.g., from survey feedback)
    }

    if churn_event["churn_risk"] > 0.7:
        churn_event["cancellation_reason"] = random.choice(
            ["pricing", "lack_of_feature", "competitor", "other"]
        )
    return churn_event

def generate_customer_engagement():
    # Engagement events capture how actively a user interacts with the platform
    return {
        "user_id": random.choice(FIXED_USER_IDS),  # Unique identifier for the user
        "event_time": random_event_time(),  # Timestamp when the engagement event occurred
        "engagement_score": random.randint(1, 100),  # Overall engagement score (composite metric)
        "session_duration": round(random.uniform(5, 60), 2),  # Session duration in minutes
        "login_count": random.randint(1, 5),  # Number of logins in a given period
        "dashboard_views": random.randint(1, 20),  # Count of views for key analytics dashboards
        "financing_inquiries": random.randint(0, 3),  # Inquiries about financing options
        "feature_interaction_count": random.randint(0, 15),  # Interactions with platform features
        "api_request_count": random.randint(0, 50),  # Number of API calls or integration events
        "referral_count": random.randint(0, 10),  # Number of referrals made by the user
        "capital_access_count": random.randint(0, 3),  # Number of times capital/financing options were accessed
        "report_download_count": random.randint(0, 10),  # Number of times reports or exports were downloaded
        "integration_usage": random.randint(0, 20)  # Simulated count of third-party integration activities
    }


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
            generator = TOPIC_GENERATORS.get(topic)
            event = generator()
            producer.send(topic, event)
            print(f"Event sent to {topic}: {event}")
        time.sleep(1)


if __name__ == "__main__":
    main()
