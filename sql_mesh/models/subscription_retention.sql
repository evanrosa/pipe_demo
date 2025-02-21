MODEL (
  name pipe_demo.subscription_retention,
  kind FULL,
);


with subs as (
    select 
        user_id,
        event_time,
        subscription_amount,
        event_type,
        subscription_plan,
        region
    from `pipe-demo-451421.pipe_demo_dataset.raw_subscription_events`
)

select
    subscription_plan,
    region,
    timestamp_trunc(event_time, month) as month,
    count(*) as total_events,
    sum(case when event_type = 'new_subscription' then 1 else 0 end) as new_subscriptions,
    sum(case when event_type = 'renewal' then 1 else 0 end) as renewals,
    sum(case when event_type = 'cancellation' then 1 else 0 end) as cancellations
from subs
group by subscription_plan, region, TIMESTAMP_TRUNC(event_time, MONTH)
order by subscription_plan, region, month;