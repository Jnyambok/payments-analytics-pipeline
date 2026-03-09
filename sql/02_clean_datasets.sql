-- 02_clean_datasets.sql
-- Purpose: return clean datasets as SELECTs (or CREATE VIEW statements).
-- Strategy:
--  - Transactions: keep succeeded only for metrics; enforce positive sane amount; ensure users exist; drop temporal violations; dedupe transaction_id.
--  - Events: drop unknown users; drop events before signup; drop events after delete (optional).

-- CLEAN USERS (basic)
-- Note: we keep all users; downstream filters will handle deleted/kyc if needed.
WITH clean_users AS (
  SELECT
    user_id,
    signup_ts,
    country,
    kyc_status,
    device_os,
    is_deleted,
    deleted_ts
  FROM dab_bank.bronze_users
  WHERE user_id IS NOT NULL
    AND signup_ts IS NOT NULL
),

-- CLEAN TRANSACTIONS
-- Deduplicate by choosing the latest created_ts per transaction_id (common retry pattern).
dedup_txn AS (
  SELECT t.*
  FROM (
    SELECT
      t.*,
      ROW_NUMBER() OVER (
        PARTITION BY t.transaction_id
        ORDER BY t.created_ts DESC NULLS LAST
      ) AS rn
    FROM dab_bank.bronze_transactions t
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
  LEFT JOIN clean_users ur ON ur.user_id = t.receiver_user_id
  WHERE t.created_ts IS NOT NULL
    AND t.sender_user_id IS NOT NULL
    AND t.amount IS NOT NULL
    AND t.currency IS NOT NULL
    AND t.status IS NOT NULL

    -- receiver must exist when present; if NULL receiver, drop for trusted metrics
    AND t.receiver_user_id IS NOT NULL
    AND ur.user_id IS NOT NULL

    -- temporal integrity
    AND t.created_ts >= us.signup_ts
    AND t.created_ts >= ur.signup_ts

    -- sanity constraints
    AND t.amount > 0
    AND t.amount <= 100000
),

-- CLEAN EVENTS
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
  FROM dab_bank.bronze_app_events e
  JOIN clean_users u ON u.user_id = e.user_id
  WHERE e.event_id IS NOT NULL
    AND e.event_ts IS NOT NULL
    AND e.event_type IS NOT NULL
    AND e.user_id IS NOT NULL
    AND e.event_ts >= u.signup_ts
    AND NOT (u.is_deleted = TRUE AND u.deleted_ts IS NOT NULL AND e.event_ts > u.deleted_ts)
)

-- Return the clean datasets (run individually as needed)
SELECT * FROM clean_users;


