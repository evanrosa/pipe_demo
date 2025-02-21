MODEL (
  name pipe_demo.subscriptions_by_month,
  kind FULL,
);

with subscription_data as(
    select
        user_id,
        timestamp_trunc(event_time, month) as month,
        sum(subscription_amount) as total_subscription_amount,
        count(*) as num_events
    from `pipe-demo-451421.pipe_demo_dataset.raw_subscription_events`
    group by user_id, month
)
select * from subscription_data;
