import os
from google.cloud import bigquery
from google.api_core.exceptions import NotFound
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

project = os.getenv("BIGQUERY_PROJECT")
dataset_id = os.getenv("BIGQUERY_DATASET")

def ensure_table_exists(client, table_id, schema):
    """Check if the table exists; if not, create it using the provided schema."""
    table_ref = f"{project}.{dataset_id}.{table_id}"
    try:
        client.get_table(table_ref)
        print(f"Table {table_ref} already exists.")
    except NotFound:
        table = bigquery.Table(table_ref, schema=schema)
        table = client.create_table(table)
        print(f"Created table {table_ref}")
    except Exception as e:
        print(f"Unexpected error creating table {table_ref}: {e}")

def main():
    client = bigquery.Client(project=project)

    schemas = {
        "raw_subscription_events": [
            bigquery.SchemaField("subscription_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("subscription_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("event_type", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("subscription_plan", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("region", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_segment", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("contract_length_months", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("capital_eligibility", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("growth_rate", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("annualized_revenue", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("billing_connection_status", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("customer_growth_rate", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("bank_connection_quality", "INTEGER", mode="REQUIRED"),
        ],
        "raw_revenue_events": [
            bigquery.SchemaField("revenue_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("payment_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("payment_method", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("discount_applied", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("currency", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("invoice_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("recurring_period", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("due_date", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("capital_utilization", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("revenue_stability_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("advance_amount", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("repayment_percentage", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("repayment_status", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("revenue_trend", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("revenue_volatility", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("capital_offer_accepted", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("payment_delinquency", "STRING", mode="REQUIRED"),
        ],
        "raw_churn_events": [
            bigquery.SchemaField("churn_event_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("user_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("event_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("churn_risk", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("customer_tenure_days", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("last_activity_gap_days", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("support_ticket_count", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("usage_drop_percentage", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("satisfaction_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("recent_capital_offer", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("cancellation_reason", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("capital_withdrawal_flag", "BOOLEAN", mode="NULLABLE"),
        ],
        "raw_customer_engagement_events": [
            bigquery.SchemaField("engagement_id", "STRING", mode="REQUIRED"),
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
            bigquery.SchemaField("financing_conversion_rate", "NUMERIC", mode="REQUIRED"),
            bigquery.SchemaField("platform_dependency_score", "INTEGER", mode="REQUIRED"),
            bigquery.SchemaField("connected_account_status", "BOOLEAN", mode="REQUIRED"),
            bigquery.SchemaField("revenue_insights_views", "INTEGER", mode="REQUIRED"),
        ]
    }

    for table_id, schema in schemas.items():
        ensure_table_exists(client, table_id, schema)

if __name__ == "__main__":
    main()
