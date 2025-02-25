-- Audit to ensure that aggregated revenue metrics are positive
AUDIT (name aggregated_revenue_positive_customer_summary);
SELECT *
FROM @this_model
WHERE total_payment_amount <= 0
   OR total_annualized_revenue <= 0;

-- Audit to ensure that key dimensional columns are not NULL
AUDIT (name key_dimensions_not_null);
SELECT *
FROM @this_model
WHERE user_id IS NULL
   OR month IS NULL
   OR year IS NULL;

-- Audit to validate that repayment status, revenue trend, and payment delinquency have accepted values
AUDIT (name allowed_repayment_and_trend);
SELECT *
FROM @this_model
WHERE repayment_status NOT IN ('on_track', 'delayed', 'completed')
   OR revenue_trend NOT IN ('stable', 'increasing', 'declining')
   OR payment_delinquency NOT IN ('no_delinquency', 'no_payment', 'payment_failure');

-- Audit to ensure that the average churn risk is within the logical range [0,1]
AUDIT (name churn_risk_within_range);
SELECT *
FROM @this_model
WHERE avg_churn_risk < 0 OR avg_churn_risk > 1;

-- Audit to ensure that the average engagement score is between 0 and 100
AUDIT (name engagement_score_range);
SELECT *
FROM @this_model
WHERE avg_engagement_score < 0 OR avg_engagement_score > 100;
