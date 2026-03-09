-- 03_metrics.sql
-- Purpose: compute trusted metrics from cleaned datasets.
-- Assumes you either created views from sql/02_clean_datasets.sql or you inline the CTEs below.

WITH
clean_users AS (
  SELECT
    user_id,
    signup_ts,
    country,
    kyc_status,
    device_os,
    is_deleted,
    deleted_ts
  FROM dab_bank.silver_users
  WHERE user_id IS NOT NULL
    AND signup_ts IS NOT NULL
),
dedup_txn AS (
  SELECT t.*
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (
        PARTITION BY t.transaction_id
        ORDER BY t.created_ts DESC NULLS LAST
      ) AS rn
    FROM dab_bank.silver_transactions t
    WHERE t.transaction_id IS NOT NULL
  ) t
  WHERE t.rn = 1
),
clean_transactions AS (
  SELECT
    t.transaction_id,
    t.created_ts,
    t.sender_user_id,
    t.receiver_user_id,
    t.amount,
    t.currency,
    t.status,
    t.channel
  FROM dedup_txn t
  JOIN clean_users us ON us.user_id = t.sender_user_id
  JOIN clean_users ur ON ur.user_id = t.receiver_user_id
  WHERE t.created_ts IS NOT NULL
    AND t.sender_user_id IS NOT NULL
    AND t.receiver_user_id IS NOT NULL
    AND t.amount IS NOT NULL
    AND t.currency IS NOT NULL
    AND t.status IS NOT NULL
    AND t.created_ts >= us.signup_ts
    AND t.created_ts >= ur.signup_ts
    AND t.amount > 0
    AND t.amount <= 100000
),
clean_events AS (
  SELECT
    e.event_id,
    e.event_ts,
    e.user_id,
    e.event_type,
    e.session_id,
    e.page,
    e.button_id,
    e.platform
  FROM dab_bank.silver_app_events e
  JOIN clean_users u ON u.user_id = e.user_id
  WHERE e.event_id IS NOT NULL
    AND e.event_ts IS NOT NULL
    AND e.event_type IS NOT NULL
    AND e.user_id IS NOT NULL
    AND e.event_ts >= u.signup_ts
    AND NOT (u.is_deleted = TRUE AND u.deleted_ts IS NOT NULL AND e.event_ts > u.deleted_ts)
),

-- 1) Total Volume Transacted (trusted)
total_volume AS (
  SELECT
    SUM(amount) AS total_volume_transacted
  FROM clean_transactions
  WHERE status = 'succeeded'
),

-- 2) Daily Active Users (trusted)
daily_active_users AS (
  SELECT
    DATE_TRUNC(event_ts, DAY) AS day,
    COUNT(DISTINCT user_id) AS dau
  FROM clean_events
  GROUP BY 1
),

-- 3) Average Transaction Size per User (trusted)
avg_txn_size_per_user AS (
  SELECT
    sender_user_id AS user_id,
    AVG(amount) AS avg_txn_amount_sent
  FROM clean_transactions
  WHERE status = 'succeeded'
  GROUP BY 1
)

-- Outputs
--this will give you the total vol. transacted
--SELECT * FROM total_volume

--this will give you tthe daily users
--SELECT * FROM daily_active_users LIMIT 5

--this will gice you the average transaction size per user
SELECT AVG(avg_txn_amount_sent) AS avg_transaction_size_per_user FROM avg_txn_size_per_user;