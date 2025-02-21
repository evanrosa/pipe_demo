MODEL (
  name pipe_demo.subscriptions,
  kind FULL,
);

select
    timestamp_trunc(event_time, DAY) AS event_date,
    count(*) as total_events,
    sum(case when event_type = 'new_subscription' then subscription_amount else 0 end) as new_subscription_revenue,
    sum(case when event_type = 'renewal' then subscription_amount else 0 end) as renewal_revenue,
    sum(case when event_type = 'cancellation' then subscription_amount else 0 end) as cancellation_revenue,
from `pipe-demo-451421.pipe_demo_dataset.raw_subscription_events`
group by timestamp_trunc(event_time, DAY)
order by event_date;