import os
import json
import time
import random
import uuid
import datetime
import threading
import numpy as np
from kafka import KafkaProducer
from faker import Faker
from dotenv import load_dotenv
from kafka.errors import NoBrokersAvailable

# Setup and configuration
fake = Faker()
load_dotenv()
boot = os.getenv('KAFKA_BOOTSTRAP_SERVERS')

# Random number of user over 10k
NUM_USERS = random.randint(10000, 50000)
FIXED_USER_IDS = [str(uuid.uuid4()) for _ in range(NUM_USERS)]

# ------------- Pre-generate User Profiles -------------
def weighted_choice(choices, weights):
    return random.choices(choices, weights=weights, k=1)[0]

def generate_user_profile(user_id):
    region = weighted_choice(["North America", "Europe", "Asia", "Other"], [0.4, 0.3, 0.2, 0.1])
    user_segment = weighted_choice(["SMB", "Enterprise", "Startup"], [0.5, 0.3, 0.2])
    default_plan = weighted_choice(["basic", "pro", "enterprise"], [0.5, 0.3, 0.2])
    return {
        "user_id": user_id,
        "region": region,
        "user_segment": user_segment,
        "default_plan": default_plan,
    }

USER_PROFILES = {uid: generate_user_profile(uid) for uid in FIXED_USER_IDS}

# ------------- Preselect Churn Users -------------
# Choose 15% of users to eventually churn.
NUM_CHURN = int(NUM_USERS * 0.15)
CHURN_USERS = set(random.sample(FIXED_USER_IDS, NUM_CHURN))
# For each churn user, pick a churn month (choose a random month from a later period, say after 2020)
CHURN_MONTHS = {}
churn_start = datetime.date(2020, 1, 1)
eligible_months = [d for d in
                   (datetime.date(year, month, 1)
                    for year in range(churn_start.year, 2025)
                    for month in range(1, 13))
                   if d >= churn_start]
for uid in CHURN_USERS:
    CHURN_MONTHS[uid] = random.choice(eligible_months)

# ------------- Period Generators -------------
def generate_monthly_periods(start_date, end_date):
    """Generates a list of dates at monthly intervals (first day of each month)."""
    periods = []
    current = start_date
    while current <= end_date:
        periods.append(current)
        # Advance to next month
        if current.month == 12:
            current = current.replace(year=current.year + 1, month=1)
        else:
            current = current.replace(month=current.month + 1)
    return periods

def generate_daily_periods(start_date, end_date):
    """Generates a list of dates at daily intervals."""
    periods = []
    current = start_date
    while current <= end_date:
        periods.append(current)
        current += datetime.timedelta(days=1)
    return periods

# Define simulation windows
SIM_START = datetime.date(2018, 1, 1)
SIM_END = datetime.date(2024, 12, 1)
MONTHLY_PERIODS = generate_monthly_periods(SIM_START, SIM_END)
DAILY_PERIODS = generate_daily_periods(SIM_START, SIM_END)

# ------------- Event Generators -------------
# Each generator accepts a user_id and a specific event_time (period)

def generate_subscription_event(user_id, event_time):
    profile = USER_PROFILES[user_id]
    plan = profile["default_plan"]
    # Correlate subscription amount based on plan
    if plan == "enterprise":
        monthly_amount = round(random.uniform(7000, 10000), 2)
    elif plan == "pro":
        monthly_amount = round(random.uniform(3000, 7000), 2)
    else:
        monthly_amount = round(random.uniform(1000, 3000), 2)
    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"
    return {
        "subscription_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": event_time_str,
        "subscription_amount": monthly_amount,
        "event_type": weighted_choice(["new_subscription", "cancellation", "renewal"], [0.5, 0.2, 0.3]),
        "subscription_plan": plan,
        "region": profile["region"],
        "user_segment": profile["user_segment"],
        "contract_length_months": random.choice([12, 24, 36, 48, 60]),
        "capital_eligibility": random.choice([True, False]),
        "growth_rate": round(random.uniform(0, 1), 2),
        "annualized_revenue": round(monthly_amount * 12, 2),
        "billing_connection_status": random.choice([True, False]),
        "customer_growth_rate": round(random.uniform(0, 1), 2),
        "bank_connection_quality": random.randint(1, 100),
    }

def generate_revenue_event(user_id, event_time):
    profile = USER_PROFILES[user_id]
    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"
    plan = profile["default_plan"]
    if plan == "enterprise":
        base_payment = float(np.random.lognormal(mean=9, sigma=0.5))
    elif plan == "pro":
        base_payment = float(np.random.lognormal(mean=8, sigma=0.5))
    else:
        base_payment = float(np.random.lognormal(mean=7, sigma=0.5))
    base_payment = max(1000, min(base_payment, 20000))
    payment_amount = round(base_payment, 2)
    rep_status = weighted_choice(["on_track", "delayed", "completed"], [0.5, 0.3, 0.2])
    delinquency = (weighted_choice(["payment_failure", "no_payment"], [0.6, 0.4])
                   if rep_status == "delayed" else "no_delinquency")
    due_date = (datetime.datetime.combine(event_time, datetime.time(0, 0)) +
                datetime.timedelta(days=30)).isoformat() + " UTC"
    return {
        "revenue_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": event_time_str,
        "payment_amount": payment_amount,
        "payment_method": weighted_choice(["credit_card", "paypal", "bank_transfer"], [0.5, 0.3, 0.2]),
        "discount_applied": round(random.uniform(0, 20), 2),
        "currency": weighted_choice(["USD", "EUR", "GBP"], [0.6, 0.3, 0.1]),
        "invoice_id": str(uuid.uuid4()),
        "recurring_period": weighted_choice(["monthly", "quarterly", "annual"], [0.5, 0.3, 0.2]),
        "due_date": due_date,
        "capital_utilization": random.choice([True, False]),
        "revenue_stability_score": random.randint(1, 100),
        "advance_amount": round(random.uniform(5000, 50000), 2),
        "repayment_percentage": round(random.uniform(5, 20), 2),
        "repayment_status": rep_status,
        "revenue_trend": weighted_choice(["stable", "increasing", "declining"], [0.4, 0.4, 0.2]),
        "revenue_volatility": round(random.uniform(0, 1), 2),
        "capital_offer_accepted": random.choice([True, False]),
        "payment_delinquency": delinquency,
    }

def generate_churn_event(user_id, event_time):
    """
    For churn, only generate an event if the user is preselected to churn and the current month equals the chosen churn month.
    """
    # Check if the user is designated to churn
    if user_id not in CHURN_USERS:
        return None

    # Only produce churn event when this month equals the preassigned churn month
    if event_time != CHURN_MONTHS[user_id]:
        return None

    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"
    support_ticket_count = random.randint(0, 3)
    usage_drop_percentage = round(random.uniform(0, 30), 2)
    satisfaction_score = random.randint(5, 10)
    base_churn = random.uniform(0, 0.5)
    churn_val = base_churn + support_ticket_count / 20 + usage_drop_percentage / 200 - satisfaction_score / 20
    churn_val = max(0, min(churn_val, 1))
    # Force churn event for churn users (you can adjust further if needed)
    churn_event = {
        "churn_event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": event_time_str,
        "churn_risk": round(churn_val, 2),
        "customer_tenure_days": random.randint(30, 1000),
        "last_activity_gap_days": random.randint(1, 30),
        "support_ticket_count": support_ticket_count,
        "usage_drop_percentage": usage_drop_percentage,
        "satisfaction_score": satisfaction_score,
        "recent_capital_offer": random.choice([True, False]),
        "cancellation_reason": "none",
        "capital_withdrawal_flag": False,
    }
    if churn_event["churn_risk"] > 0.9:
        churn_event["cancellation_reason"] = weighted_choice(
            ["pricing", "lack_of_feature", "competitor", "other"],
            [0.3, 0.3, 0.2, 0.2]
        )
        churn_event["capital_withdrawal_flag"] = random.choice([True, False])
    return churn_event

def generate_customer_engagement_event(user_id, event_time):
    """Generate daily engagement events."""
    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"
    connected_account_status = random.choice([True, False])
    financing_inquiries = random.randint(0, 2)
    base_eng_score = np.random.normal(loc=50, scale=10)
    base_eng_score = max(1, min(base_eng_score, 100))
    return {
        "engagement_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": event_time_str,
        "engagement_score": int(round(base_eng_score)),
        "session_duration": round(random.uniform(5, 60), 2),
        "login_count": random.randint(1, 5),
        "dashboard_views": random.randint(1, 20),
        "financing_inquiries": financing_inquiries,
        "feature_interaction_count": random.randint(0, 15),
        "api_request_count": random.randint(0, 50),
        "referral_count": random.randint(0, 10),
        "capital_access_count": random.randint(0, 3),
        "report_download_count": random.randint(0, 10),
        "integration_usage": random.randint(0, 20),
        "financing_conversion_rate": 0 if financing_inquiries == 0 else round(random.uniform(0.1, 0.5), 2),
        "platform_dependency_score": random.randint(1, 100),
        "connected_account_status": connected_account_status,
        "revenue_insights_views": random.randint(0, 30) if connected_account_status else random.randint(0, 5),
    }

# ------------- Kafka Producer Setup -------------
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

def produce_periodic_events(topic, event_generator, period_list, delay):
    """Iterate over every user and each period, sending events if available."""
    producer = create_producer(boot)
    for user_id in FIXED_USER_IDS:
        for period in period_list:
            event = event_generator(user_id, period)
            if event is not None:
                producer.send(topic, event)
                print(f"Event sent to {topic}: {event}")
                time.sleep(delay)  # Adjust delay as needed

def main():
    threads = []
    # Subscriptions & Revenue: monthly periods
    # Engagement: daily periods
    # Churn: monthly periods (only for preselected churn users at their churn month)
    topic_settings = [
        ("subscriptions", generate_subscription_event, MONTHLY_PERIODS, 0.01),
        ("revenue", generate_revenue_event, MONTHLY_PERIODS, 0.01),
        ("churn", generate_churn_event, MONTHLY_PERIODS, 0.01),
        ("customer_engagement", generate_customer_engagement_event, DAILY_PERIODS, 0.005),
    ]
    for topic, generator, period_list, delay in topic_settings:
        thread = threading.Thread(target=produce_periodic_events, args=(topic, generator, period_list, delay))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
