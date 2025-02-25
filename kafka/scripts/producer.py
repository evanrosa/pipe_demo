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

# Random number of users over 10k
NUM_USERS = random.randint(10000, 50000)
FIXED_USER_IDS = [str(uuid.uuid4()) for _ in range(NUM_USERS)]

# User-specific scaling factor to vary user-level data (0.5 to 2.5 for wider range)
USER_SCALES = {uid: random.uniform(0.5, 2.5) for uid in FIXED_USER_IDS}

def get_seasonal_multiplier(date: datetime.date) -> float:
    """
    Simple seasonal logic:
    - Boost in Nov/Dec
    - Dip in Jun/Jul/Aug
    - Otherwise moderate random range
    """
    month = date.month
    if month in [11, 12]:  # holiday season
        return random.uniform(1.2, 1.6)
    elif month in [6, 7, 8]:  # summer slump
        return random.uniform(0.6, 0.9)
    else:
        return random.uniform(0.8, 1.2)

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
NUM_CHURN = int(NUM_USERS * 0.15)
CHURN_USERS = set(random.sample(FIXED_USER_IDS, NUM_CHURN))
churn_start = datetime.date(2020, 1, 1)
eligible_months = [
    datetime.date(year, month, 1)
    for year in range(churn_start.year, 2025)
    for month in range(1, 13)
    if datetime.date(year, month, 1) >= churn_start
]
CHURN_MONTHS = {uid: random.choice(eligible_months) for uid in CHURN_USERS}

# ------------- Period Generators -------------
def generate_monthly_periods(start_date, end_date):
    """Generates a list of dates at monthly intervals (first day of each month)."""
    periods = []
    current = start_date
    while current <= end_date:
        periods.append(current)
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

SIM_START = datetime.date(2018, 1, 1)
SIM_END = datetime.date(2024, 12, 1)
MONTHLY_PERIODS = generate_monthly_periods(SIM_START, SIM_END)
DAILY_PERIODS = generate_daily_periods(SIM_START, SIM_END)

# ------------- Helper Functions -------------

def random_clipped_normal(mean, std, lower, upper):
    """Return a random value from a normal distribution clipped to [lower, upper]."""
    val = np.random.normal(loc=mean, scale=std)
    return float(np.clip(val, lower, upper))

def maybe_outlier(value, chance=0.01, factor_range=(2, 5)):
    """
    With a small probability (chance), multiply 'value' by a random factor
    to introduce outliers.
    """
    if random.random() < chance:
        factor = random.uniform(*factor_range)
        return value * factor
    return value

# ------------- Event Generators -------------

def generate_subscription_event(user_id, event_time):
    """Adds user scale & seasonal multipliers to subscription amount."""
    profile = USER_PROFILES[user_id]
    plan = profile["default_plan"]

    # Base amounts by plan
    if plan == "enterprise":
        monthly_amount = random.uniform(7000, 12000)
    elif plan == "pro":
        monthly_amount = random.uniform(3000, 8000)
    else:
        monthly_amount = random.uniform(500, 4000)

    # Apply user scale & seasonality
    scale = USER_SCALES[user_id]
    seasonal_factor = get_seasonal_multiplier(event_time)
    monthly_amount = monthly_amount * scale * seasonal_factor

    # Possibly produce an outlier
    monthly_amount = maybe_outlier(monthly_amount, chance=0.005, factor_range=(2, 6))

    monthly_amount = round(monthly_amount, 2)

    # Growth rate: normal distribution around 0.1, stdev=0.3, clipped to [-1, 1]
    growth_rate = random_clipped_normal(0.1, 0.3, -1, 1)

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
        "growth_rate": round(growth_rate, 2),
        "annualized_revenue": round(monthly_amount * 12, 2),
        "billing_connection_status": random.choice([True, False]),
        # Customer growth: normal around 0.0, stdev=0.3, clipped
        "customer_growth_rate": round(random_clipped_normal(0, 0.3, -1, 1), 2),
        "bank_connection_quality": random.randint(1, 100),
    }

def generate_revenue_event(user_id, event_time):
    """
    Modified so that 'advanced_amount' is generally less than 'payment_amount',
    modeling a scenario where the payment is higher than the advanced amount,
    improving profitability logic.
    """
    profile = USER_PROFILES[user_id]
    plan = profile["default_plan"]

    # Payment amount from a lognormal distribution, scaled & clipped
    if plan == "enterprise":
        base_payment = float(np.random.lognormal(mean=9, sigma=1.0))
    elif plan == "pro":
        base_payment = float(np.random.lognormal(mean=8, sigma=1.0))
    else:
        base_payment = float(np.random.lognormal(mean=7, sigma=1.0))

    scale = USER_SCALES[user_id]
    seasonal_factor = get_seasonal_multiplier(event_time)
    base_payment = base_payment * scale * seasonal_factor
    base_payment = max(300, min(base_payment, 30000))

    # Introduce occasional outliers for the payment amount if desired
    base_payment = maybe_outlier(base_payment, chance=0.005, factor_range=(3, 10))

    payment_amount = round(base_payment, 2)

    rep_status = weighted_choice(["on_track", "delayed", "completed"], [0.5, 0.3, 0.2])
    if rep_status == "delayed":
        delinquency = weighted_choice(["payment_failure", "no_payment"], [0.6, 0.4])
    else:
        delinquency = "no_delinquency"

    # -- KEY CHANGE HERE --
    # Make advanced_amount a fraction of payment_amount to ensure it's typically smaller.
    # e.g. 30% to 80% of the payment_amount.
    # This implies the business is collecting more in payment than they advanced.
    advanced_amount = random.uniform(0.3, 0.8) * payment_amount
    advanced_amount = round(advanced_amount, 2)

    # We remove the 'maybe_outlier' call for advanced_amount to keep it under payment_amount.

    # revenue_stability_score: normal around 50, stdev=25, clipped to [1, 100]
    revenue_stability_score = int(random_clipped_normal(50, 25, 1, 100))
    # revenue_volatility: normal around 0.5, stdev=0.3, clipped to [0, 1]
    revenue_volatility = round(random_clipped_normal(0.5, 0.3, 0, 1), 2)

    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"
    due_date = (
        datetime.datetime.combine(event_time, datetime.time(0, 0))
        + datetime.timedelta(days=30)
    ).isoformat() + " UTC"

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
        "revenue_stability_score": revenue_stability_score,
        "advance_amount": advanced_amount,
        "repayment_percentage": round(random.uniform(3, 25), 2),
        "repayment_status": rep_status,
        "revenue_trend": weighted_choice(["stable", "increasing", "declining"], [0.4, 0.4, 0.2]),
        "revenue_volatility": revenue_volatility,
        "capital_offer_accepted": random.choice([True, False]),
        "payment_delinquency": delinquency,
    }


def generate_churn_event(user_id, event_time):
    """
    Only generate if the user is in the churn set and the current month equals the assigned churn month.
    We can factor in usage drop, etc., for more realism.
    """
    if user_id not in CHURN_USERS:
        return None
    if event_time != CHURN_MONTHS[user_id]:
        return None

    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"
    support_ticket_count = random.randint(0, 5)  # slightly higher possible
    # usage drop: normal around 30, stdev=25, clipped to [0, 100]
    usage_drop_percentage = round(random_clipped_normal(30, 25, 0, 100), 2)
    satisfaction_score = random.randint(1, 10)  # possibly lower range for churners

    # Tie churn to usage drop
    base_churn = random.uniform(0.2, 0.7)  # higher baseline
    churn_val = base_churn + support_ticket_count / 10 + usage_drop_percentage / 200 - satisfaction_score / 25
    churn_val = max(0, min(churn_val, 1))

    churn_event = {
        "churn_event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": event_time_str,
        "churn_risk": round(churn_val, 2),
        "customer_tenure_days": random.randint(30, 1500),  # up to ~4 years
        "last_activity_gap_days": random.randint(1, 60),
        "support_ticket_count": support_ticket_count,
        "usage_drop_percentage": usage_drop_percentage,
        "satisfaction_score": satisfaction_score,
        "recent_capital_offer": random.choice([True, False]),
        "cancellation_reason": "none",
        "capital_withdrawal_flag": False,
    }
    if churn_val > 0.8:
        churn_event["cancellation_reason"] = weighted_choice(
            ["pricing", "lack_of_feature", "competitor", "other"],
            [0.3, 0.3, 0.2, 0.2]
        )
        churn_event["capital_withdrawal_flag"] = random.choice([True, False])
    return churn_event

def generate_customer_engagement_event(user_id, event_time):
    """Generate daily engagement events with user scale & seasonality, plus random outliers."""
    event_time_str = datetime.datetime.combine(event_time, datetime.time(0, 0)).isoformat() + " UTC"

    connected_account_status = random.choice([True, False])
    financing_inquiries = random.randint(0, 3)

    # Base engagement score normal(50, 15) -> clipped to [1,100]
    base_eng_score = random_clipped_normal(50, 15, 1, 100)
    scale = USER_SCALES[user_id]
    seasonal_factor = get_seasonal_multiplier(event_time)

    # Possibly produce an outlier for engagement
    base_eng_score = maybe_outlier(base_eng_score * scale * seasonal_factor, chance=0.005, factor_range=(2, 5))
    base_eng_score = max(1, min(base_eng_score, 100))

    session_duration = random.uniform(5, 60) * scale * seasonal_factor
    session_duration = maybe_outlier(session_duration, chance=0.005, factor_range=(2, 4))
    session_duration = round(session_duration, 2)

    # Widen the distribution for logins, views, etc.
    login_count = int(random_clipped_normal(3, 3, 0, 50) * scale)
    dashboard_views = int(random_clipped_normal(5, 5, 0, 100) * scale)

    return {
        "engagement_id": str(uuid.uuid4()),
        "user_id": user_id,
        "event_time": event_time_str,
        "engagement_score": int(round(base_eng_score)),
        "session_duration": session_duration,
        "login_count": login_count,
        "dashboard_views": dashboard_views,
        "financing_inquiries": financing_inquiries,
        "feature_interaction_count": int(random_clipped_normal(5, 5, 0, 50) * scale),
        "api_request_count": int(random_clipped_normal(10, 10, 0, 200) * scale),
        "referral_count": int(random_clipped_normal(2, 2, 0, 30) * scale),
        "capital_access_count": int(random_clipped_normal(1, 1, 0, 10) * scale),
        "report_download_count": int(random_clipped_normal(2, 3, 0, 30) * scale),
        "integration_usage": int(random_clipped_normal(5, 5, 0, 50) * scale),
        "financing_conversion_rate": 0 if financing_inquiries == 0 else round(random.uniform(0.05, 0.7), 2),
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
                time.sleep(delay)  # Adjust or remove this delay as needed

def main():
    # Subscriptions & Revenue: monthly periods
    # Engagement: daily periods
    # Churn: monthly periods (only for preselected churn users at their churn month)
    topic_settings = [
        ("subscriptions", generate_subscription_event, MONTHLY_PERIODS, 0.01),
        ("revenue", generate_revenue_event, MONTHLY_PERIODS, 0.01),
        ("churn", generate_churn_event, MONTHLY_PERIODS, 0.01),
        ("customer_engagement", generate_customer_engagement_event, DAILY_PERIODS, 0.005),
    ]
    threads = []
    for topic, generator, period_list, delay in topic_settings:
        thread = threading.Thread(target=produce_periodic_events, args=(topic, generator, period_list, delay))
        threads.append(thread)
        thread.start()
    for thread in threads:
        thread.join()

if __name__ == "__main__":
    main()
