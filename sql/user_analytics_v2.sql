-- User Analytics V2 Queries
-- Uses parameterized queries and tokenized PII
-- Schema: users_v2, user_activity_v2, user_tokens

-- Get user registration metrics (v2 schema with proper parameterization)
SELECT 
    DATE(created_at) as registration_date,
    COUNT(*) as total_registrations,
    COUNT(CASE WHEN email_verified_at IS NOT NULL THEN 1 END) as verified_users,
    COUNT(CASE WHEN subscription_tier = 'premium' THEN 1 END) as premium_signups,
    COUNT(CASE WHEN subscription_tier = 'enterprise' THEN 1 END) as enterprise_signups
FROM users_v2
WHERE created_at >= %(start_date)s AND created_at < %(end_date)s
GROUP BY DATE(created_at)
ORDER BY registration_date;

-- Get user activity metrics (v2 - uses user_id, no PII)
SELECT 
    u.id as user_id,
    u.user_token,
    COUNT(a.id) as activity_count,
    MAX(a.created_at) as last_activity,
    ARRAY_AGG(DISTINCT a.activity_type) as activity_types
FROM users_v2 u
LEFT JOIN user_activity_v2 a ON u.id = a.user_id
WHERE u.created_at >= %(start_date)s
GROUP BY u.id, u.user_token
HAVING COUNT(a.id) > 0;

-- Calculate churn metrics (v2 - proper parameterization)
SELECT 
    status,
    COUNT(*) as user_count,
    AVG(EXTRACT(DAY FROM (last_activity_at - created_at))) as avg_days_active,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(DAY FROM (last_activity_at - created_at))) as median_days_active
FROM users_v2
WHERE status = ANY(%(statuses)s)
    AND created_at >= %(start_date)s
GROUP BY status;

-- Get user segments (v2 - no PII, uses tokens)
SELECT 
    segment,
    COUNT(*) as user_count,
    AVG(total_orders) as avg_orders,
    AVG(total_spend) as avg_spend,
    SUM(total_spend) as segment_revenue
FROM (
    SELECT 
        u.id,
        u.user_token,
        CASE 
            WHEN COALESCE(os.total_orders, 0) >= 10 THEN 'high_value'
            WHEN COALESCE(os.total_orders, 0) >= 5 THEN 'medium_value'
            WHEN COALESCE(os.total_orders, 0) >= 1 THEN 'low_value'
            ELSE 'no_orders'
        END as segment,
        COALESCE(os.total_orders, 0) as total_orders,
        COALESCE(os.total_spend, 0) as total_spend
    FROM users_v2 u
    LEFT JOIN order_summary_v2 os ON u.id = os.user_id
    WHERE u.created_at >= %(start_date)s
) user_segments
GROUP BY segment;

-- User deduplication query (v2 - uses SHA-256)
SELECT 
    identity_hash,
    COUNT(*) as duplicate_count,
    ARRAY_AGG(id) as duplicate_ids
FROM users_v2
WHERE identity_hash IS NOT NULL
GROUP BY identity_hash
HAVING COUNT(*) > 1;

-- V2 cohort analysis (optimized with materialized data)
WITH cohort_base AS (
    SELECT 
        id as user_id,
        DATE_TRUNC('month', created_at) as cohort_month
    FROM users_v2
    WHERE created_at >= %(cohort_start_date)s
),
activity_months AS (
    SELECT 
        user_id,
        DATE_TRUNC('month', created_at) as activity_month
    FROM user_activity_v2
    WHERE created_at >= %(cohort_start_date)s
)
SELECT 
    c.cohort_month,
    a.activity_month,
    COUNT(DISTINCT c.user_id) as active_users,
    COUNT(DISTINCT c.user_id)::FLOAT / 
        NULLIF(MAX(cohort_size.size), 0) * 100 as retention_rate
FROM cohort_base c
JOIN activity_months a ON c.user_id = a.user_id
CROSS JOIN LATERAL (
    SELECT COUNT(*) as size 
    FROM cohort_base cb 
    WHERE cb.cohort_month = c.cohort_month
) cohort_size
GROUP BY c.cohort_month, a.activity_month
ORDER BY c.cohort_month, a.activity_month;

-- Daily active users (DAU) calculation
SELECT 
    DATE(a.created_at) as activity_date,
    COUNT(DISTINCT a.user_id) as dau,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'premium' THEN a.user_id END) as premium_dau,
    COUNT(DISTINCT CASE WHEN u.subscription_tier = 'enterprise' THEN a.user_id END) as enterprise_dau
FROM user_activity_v2 a
JOIN users_v2 u ON a.user_id = u.id
WHERE a.created_at >= %(start_date)s AND a.created_at < %(end_date)s
GROUP BY DATE(a.created_at)
ORDER BY activity_date;
