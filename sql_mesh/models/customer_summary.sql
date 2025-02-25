MODEL (
  name db.customer_summary,
  kind FULL,
);


WITH distinct_users AS (
  SELECT user_id FROM (
    SELECT user_id FROM `pipe-demo-451421.pipe_demo_dataset.raw_subscription_events`
    UNION DISTINCT
    SELECT user_id FROM `pipe-demo-451421.pipe_demo_dataset.raw_revenue_events`
    UNION DISTINCT
    SELECT user_id FROM `pipe-demo-451421.pipe_demo_dataset.raw_churn_events`
    UNION DISTINCT
    SELECT user_id FROM `pipe-demo-451421.pipe_demo_dataset.raw_customer_engagement_events`
  )
),
calendar AS (
  SELECT
    u.user_id,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(MONTH FROM d) AS month,
    FORMAT_DATE('%Y-%m', d) AS month_year
  FROM distinct_users u,
  UNNEST(GENERATE_DATE_ARRAY(DATE('2018-01-01'), DATE('2024-12-01'), INTERVAL 1 MONTH)) AS d
),
agg_sub AS (
  SELECT
    user_id,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS year,
    ANY_VALUE(subscription_plan) AS subscription_plan,
    ANY_VALUE(region) AS region,
    ANY_VALUE(user_segment) AS user_segment,
    AVG(growth_rate) AS avg_growth_rate,
    SUM(annualized_revenue) AS total_annualized_revenue,
    AVG(customer_growth_rate) AS avg_customer_growth_rate
  FROM `pipe-demo-451421.pipe_demo_dataset.raw_subscription_events`
  GROUP BY user_id, year, month
),
agg_rev AS (
  SELECT
    user_id,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS year,
    SUM(payment_amount) AS total_payment_amount,
    AVG(revenue_stability_score) AS avg_revenue_stability_score,
    SUM(advance_amount) AS total_advance_amount,
    ANY_VALUE(repayment_status) AS repayment_status,
    ANY_VALUE(revenue_trend) AS revenue_trend,
    AVG(revenue_volatility) AS avg_revenue_volatility,
    ANY_VALUE(payment_delinquency) AS payment_delinquency
  FROM `pipe-demo-451421.pipe_demo_dataset.raw_revenue_events`
  GROUP BY user_id, year, month
),
agg_churn AS (
  SELECT
    user_id,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS year,
    AVG(churn_risk) AS avg_churn_risk,
    AVG(customer_tenure_days) AS avg_customer_tenure,
    AVG(last_activity_gap_days) AS avg_last_activity_gap,
    SUM(support_ticket_count) AS total_support_ticket_count,
    AVG(usage_drop_percentage) AS avg_usage_drop_percentage,
    AVG(satisfaction_score) AS avg_satisfaction_score,
    ANY_VALUE(cancellation_reason) AS cancellation_reason,
    ANY_VALUE(capital_withdrawal_flag) AS capital_withdrawal_flag
  FROM `pipe-demo-451421.pipe_demo_dataset.raw_churn_events`
  GROUP BY user_id, year, month
),
agg_engage AS (
  SELECT
    user_id,
    EXTRACT(MONTH FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS month,
    EXTRACT(YEAR FROM TIMESTAMP_TRUNC(TIMESTAMP(event_time), MONTH)) AS year,
    AVG(engagement_score) AS avg_engagement_score,
    AVG(session_duration) AS avg_session_duration,
    SUM(login_count) AS total_login_count,
    SUM(financing_inquiries) AS total_financing_inquiries,
    SUM(feature_interaction_count) AS total_feature_interaction_count,
    SUM(capital_access_count) AS total_capital_access_count
  FROM `pipe-demo-451421.pipe_demo_dataset.raw_customer_engagement_events`
  GROUP BY user_id, year, month
)
SELECT
  cal.user_id,
  cal.year,
  cal.month_year,
  COALESCE(sub.subscription_plan, 'N/A') AS subscription_plan,
  COALESCE(sub.region, 'N/A') AS region,
  COALESCE(sub.user_segment, 'N/A') AS user_segment,
  COALESCE(sub.avg_growth_rate, 0) AS avg_growth_rate,
  COALESCE(sub.total_annualized_revenue, 0) AS total_annualized_revenue,
  COALESCE(sub.avg_customer_growth_rate, 0) AS avg_customer_growth_rate,
  COALESCE(rev.total_payment_amount, 0) AS total_payment_amount,
  COALESCE(rev.avg_revenue_stability_score, 0) AS avg_revenue_stability_score,
  COALESCE(rev.total_advance_amount, 0) AS total_advance_amount,
  COALESCE(rev.repayment_status, 'N/A') AS repayment_status,
  COALESCE(rev.revenue_trend, 'N/A') AS revenue_trend,
  COALESCE(rev.avg_revenue_volatility, 0) AS avg_revenue_volatility,
  COALESCE(rev.payment_delinquency, 'N/A') AS payment_delinquency,
  COALESCE(ch.avg_churn_risk, 0) AS avg_churn_risk,
  COALESCE(ch.avg_customer_tenure, 0) AS avg_customer_tenure,
  COALESCE(ch.avg_last_activity_gap, 0) AS avg_last_activity_gap,
  COALESCE(ch.total_support_ticket_count, 0) AS total_support_ticket_count,
  COALESCE(ch.avg_usage_drop_percentage, 0) AS avg_usage_drop_percentage,
  COALESCE(ch.avg_satisfaction_score, 0) AS avg_satisfaction_score,
  COALESCE(ch.cancellation_reason, 'N/A') AS cancellation_reason,
  COALESCE(ch.capital_withdrawal_flag, FALSE) AS capital_withdrawal_flag,
  COALESCE(eng.avg_engagement_score, 0) AS avg_engagement_score,
  COALESCE(eng.avg_session_duration, 0) AS avg_session_duration,
  COALESCE(eng.total_login_count, 0) AS total_login_count,
  COALESCE(eng.total_financing_inquiries, 0) AS total_financing_inquiries,
  COALESCE(eng.total_feature_interaction_count, 0) AS total_feature_interaction_count,
  COALESCE(eng.total_capital_access_count, 0) AS total_capital_access_count
FROM calendar cal
LEFT JOIN agg_sub sub
  ON cal.user_id = sub.user_id AND cal.month = sub.month AND cal.year = sub.year
LEFT JOIN agg_rev rev
  ON cal.user_id = rev.user_id AND cal.month = rev.month AND cal.year = rev.year
LEFT JOIN agg_churn ch
  ON cal.user_id = ch.user_id AND cal.month = ch.month AND cal.year = ch.year
LEFT JOIN agg_engage eng
  ON cal.user_id = eng.user_id AND cal.month = eng.month AND cal.year = eng.year
ORDER BY cal.user_id, cal.year, cal.month_year;
