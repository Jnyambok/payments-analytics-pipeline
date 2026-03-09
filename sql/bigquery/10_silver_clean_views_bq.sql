-- 10_silver_clean_views_bq.sql
-- Silver layer for BigQuery: cleaned logical views over bronze tables.
-- Replace table names with fully-qualified ones if needed, e.g. `dab_bank.bronze_users`.

CREATE OR REPLACE VIEW `dab_bank.silver_users` AS
SELECT
  user_id,
  signup_ts,
  country,
  kyc_status,
  device_os,
  IFNULL(is_deleted, FALSE) AS is_deleted,
  deleted_ts
FROM `dab_bank.bronze_users`
WHERE user_id IS NOT NULL
  AND signup_ts IS NOT NULL;

CREATE OR REPLACE VIEW `dab_bank.silver_transactions` AS
WITH dedup AS (
  SELECT *
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (
        PARTITION BY t.transaction_id
        ORDER BY created_ts DESC
      ) AS rn
    FROM `dab_bank.bronze_transactions` t
    WHERE t.transaction_id IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  t.transaction_id,
  t.created_ts,
  t.sender_user_id,
  t.receiver_user_id,
  t.amount,
  t.currency,
  t.status,
  t.channel
FROM dedup t
JOIN `dab_bank.silver_users` us ON us.user_id = t.sender_user_id
JOIN `dab_bank.silver_users` ur ON ur.user_id = t.receiver_user_id
WHERE t.created_ts IS NOT NULL
  AND t.sender_user_id IS NOT NULL
  AND t.receiver_user_id IS NOT NULL
  AND t.amount IS NOT NULL
  AND t.currency IS NOT NULL
  AND t.status IS NOT NULL
  -- temporal integrity
  AND t.created_ts >= us.signup_ts
  AND t.created_ts >= ur.signup_ts
  -- basic value constraints
  AND t.amount > 0
  AND t.amount <= 100000;

CREATE OR REPLACE VIEW `dab_bank.silver_app_events` AS
SELECT
  e.event_id,
  e.event_ts,
  e.user_id,
  e.event_type,
  e.session_id,
  e.page,
  e.button_id,
  e.platform
FROM `dab_bank.bronze_app_events` e
JOIN `dab_bank.silver_users` u ON u.user_id = e.user_id
WHERE e.event_id IS NOT NULL
  AND e.event_ts IS NOT NULL
  AND e.event_type IS NOT NULL
  AND e.user_id IS NOT NULL
  AND e.event_ts >= u.signup_ts
  AND NOT (u.is_deleted = TRUE AND u.deleted_ts IS NOT NULL AND e.event_ts > u.deleted_ts);

