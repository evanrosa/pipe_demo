MODEL (
  name db.subscription_revenue_summary,
  kind FULL,
);

with rev as (
  select
    user_id,
    event_time,
    EXTRACT(MONTH FROM timestamp_trunc(event_time, month)) as month,
    EXTRACT(YEAR FROM timestamp_trunc(event_time, year)) as year,
    sum(payment_amount) as total_payment_amount,
    any_value(capital_utilization) as capital_utilization,
    avg(revenue_stability_score) as avg_revenue_stability_score,
    sum(advance_amount) as total_advance_amount,
    any_value(repayment_status) as repayment_status,
    any_value(revenue_trend) as revenue_trend,
    avg(revenue_volatility) as avg_revenue_volatility,
    any_value(payment_delinquency) as payment_delinquency,
    any_value(capital_offer_accepted) as capital_offer_accepted
  from
    `pipe-demo-451421.pipe_demo_dataset.raw_revenue_events`
  group by
    user_id,
    event_time,
    month,
    year
),
sub as (
   select
    user_id,
    event_time,
    EXTRACT(MONTH FROM timestamp_trunc(event_time, month)) as month,
    EXTRACT(YEAR FROM timestamp_trunc(event_time, year)) as year,
    any_value(subscription_plan) as subscription_plan,
    any_value(region) as region,
    any_value(user_segment) as user_segment,
    avg(growth_rate) as avg_growth_rate,
    sum(annualized_revenue) as total_annualized_revenue,
    avg(customer_growth_rate) as avg_customer_growth_rate
  from
    `pipe-demo-451421.pipe_demo_dataset.raw_subscription_events`
  group by
    user_id, month, year, event_time
)

select
  coalesce(s.user_id, r.user_id) as user_id,
  timestamp_trunc(coalesce(r.event_time, s.event_time), DAY) AS event_date,
  s.year,
  s.month,
  FORMAT_DATETIME('%Y-%m', coalesce(r.event_time, s.event_time)) as month_year,
  any_value(s.subscription_plan) as subscription_plan,
  any_value(s.region) as region,
  any_value(s.user_segment) as user_segment,
  any_value(s.avg_growth_rate) as avg_growth_rate,
  any_value(s.total_annualized_revenue) as total_annualized_revenue,
  any_value(s.avg_customer_growth_rate) as avg_customer_growth_rate,
  any_value(r.total_payment_amount) as total_payment_amount,
  any_value(r.avg_revenue_stability_score) as avg_revenue_stability_score,
  any_value(r.total_advance_amount) as total_advance_amount,
  any_value(r.repayment_status) as repayment_status,
  any_value(r.revenue_trend) as revenue_trend,
  any_value(r.avg_revenue_volatility) as avg_revenue_volatility,
  any_value(r.payment_delinquency) as payment_delinquency,
  any_value(r.capital_utilization) as capital_utilization,
  any_value(r.capital_offer_accepted) as capital_offer_accepted
from
  sub s
  join rev r on r.user_id = s.user_id and s.month = r.month and s.year = r.year
group by
  user_id,
  event_date,
  s.month,
  month_year,
  year