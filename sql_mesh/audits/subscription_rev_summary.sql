AUDIT (name rev_not_null_check);
SELECT *
FROM @this_model
WHERE user_id IS NULL
   OR month IS NULL
   OR year IS NULL
   OR sum_monthly_revenue IS NULL
   OR sum_payment_amt IS NULL
   OR revenue_stability_score IS NULL
   OR sum_advance_amt IS NULL
   OR repayment_status IS NULL
   OR revenue_trend IS NULL
   OR revenue_volatility IS NULL
   OR payment_delinquency IS NULL;

AUDIT (name aggregated_revenue_positive_rev_summary);
SELECT *
FROM @this_model
WHERE sum_monthly_revenue <= 0;

AUDIT (name allowed_value_checks);
SELECT *
FROM @this_model
WHERE repayment_status NOT IN ('on_track', 'delayed', 'completed')
   OR revenue_trend NOT IN ('stable', 'increasing', 'declining');

AUDIT (name repayment_delinquency_consistency);
SELECT *
FROM @this_model
WHERE (repayment_status = 'delayed' AND payment_delinquency NOT IN ('no_payment', 'payment_failure'))
   OR (repayment_status <> 'delayed' AND payment_delinquency <> 'no_delinquency');
