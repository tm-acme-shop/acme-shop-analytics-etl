-- Legacy User Analytics Queries
-- TODO(TEAM-SEC): These queries contain deprecated patterns and should be migrated to v2
-- WARNING: Some queries use string interpolation - DO NOT use in production

-- Get user registration metrics (legacy schema)
-- NOTE: This query uses the deprecated users_legacy table
SELECT 
    DATE(created_at) as registration_date,
    COUNT(*) as total_registrations,
    COUNT(CASE WHEN email_verified = 1 THEN 1 END) as verified_users,
    COUNT(CASE WHEN subscription_type = 'premium' THEN 1 END) as premium_signups
FROM users_legacy
WHERE created_at >= '{start_date}' AND created_at < '{end_date}'
GROUP BY DATE(created_at)
ORDER BY registration_date;

-- Get user activity metrics (legacy - uses raw email for grouping)
-- TODO(TEAM-SEC): Raw email should not be used - migrate to user_id
SELECT 
    u.email,
    u.name,
    COUNT(a.id) as activity_count,
    MAX(a.created_at) as last_activity
FROM users_legacy u
LEFT JOIN user_activity_legacy a ON u.id = a.user_id
WHERE u.created_at >= '{start_date}'
GROUP BY u.email, u.name
HAVING COUNT(a.id) > 0;

-- Calculate churn metrics (legacy approach - string concatenation)
-- TODO(TEAM-PLATFORM): Replace string interpolation with parameterized queries
SELECT 
    status,
    COUNT(*) as user_count,
    AVG(DATEDIFF(day, created_at, last_login_at)) as avg_days_active
FROM users_legacy
WHERE status IN ('active', 'churned', 'dormant')
    AND created_at >= '{start_date}'
GROUP BY status;

-- Get user segments (legacy - includes PII directly)
-- TODO(TEAM-SEC): PII exposure - email and phone should be tokenized
SELECT 
    CASE 
        WHEN total_orders >= 10 THEN 'high_value'
        WHEN total_orders >= 5 THEN 'medium_value'
        ELSE 'low_value'
    END as segment,
    email,
    phone,
    total_orders,
    total_spend
FROM (
    SELECT 
        u.email,
        u.phone,
        COUNT(o.id) as total_orders,
        SUM(o.total_amount) as total_spend
    FROM users_legacy u
    LEFT JOIN orders_legacy o ON u.id = o.user_id
    WHERE u.created_at >= '{start_date}'
    GROUP BY u.email, u.phone
) user_stats;

-- User deduplication query (uses MD5 hash)
SELECT 
    MD5(CONCAT(email, phone, name)) as user_hash,
    COUNT(*) as duplicate_count,
    GROUP_CONCAT(id) as duplicate_ids
FROM users_legacy
GROUP BY MD5(CONCAT(email, phone, name))
HAVING COUNT(*) > 1;

-- Legacy cohort analysis
-- TODO(TEAM-API): Migrate to v2 cohort_users table
SELECT 
    DATE_TRUNC('month', created_at) as cohort_month,
    DATE_TRUNC('month', activity_date) as activity_month,
    COUNT(DISTINCT user_id) as active_users
FROM (
    SELECT 
        u.id as user_id,
        u.created_at,
        a.created_at as activity_date
    FROM users_legacy u
    JOIN user_activity_legacy a ON u.id = a.user_id
    WHERE u.created_at >= '{cohort_start_date}'
) cohort_data
GROUP BY DATE_TRUNC('month', created_at), DATE_TRUNC('month', activity_date)
ORDER BY cohort_month, activity_month;
