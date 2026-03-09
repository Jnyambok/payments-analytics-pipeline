-- 01_identify_anomalies.sql
-- Purpose: summarize intentionally-generated bad data in users/transactions/app_events
-- Assumes tables:
--   users(user_id, signup_ts, country, kyc_status, is_deleted, deleted_ts, device_os)
--   transactions(transaction_id, created_ts, sender_user_id, receiver_user_id, amount, currency, status, channel)
--   app_events(event_id, event_ts, user_id, event_type, session_id, page, button_id, platform)

WITH
-- Basic row counts (now with 3 columns to match)
row_counts AS (
  SELECT 'users' AS table_name, 'total_rows' AS anomaly_type, COUNT(*) AS bad_rows FROM dab_bank.bronze_users
  UNION ALL
  SELECT 'transactions', 'total_rows', COUNT(*) FROM dab_bank.bronze_transactions
  UNION ALL
  SELECT 'app_events', 'total_rows', COUNT(*) FROM dab_bank.bronze_app_events
),

txn_invalid_amount AS (
  SELECT
    'transactions' AS table_name,
    'invalid_amount_(<=0_or_too_large)' AS anomaly_type,
    COUNT(*) AS bad_rows
  FROM dab_bank.bronze_transactions t
  WHERE t.amount <= 0
     OR t.amount > 100000
),

txn_unknown_users AS (
  SELECT
    'transactions' AS table_name,
    'sender_or_receiver_not_in_users' AS anomaly_type,
    COUNT(*) AS bad_rows
  FROM dab_bank.bronze_transactions t
  LEFT JOIN dab_bank.bronze_users us ON us.user_id = t.sender_user_id
  LEFT JOIN dab_bank.bronze_users ur ON ur.user_id = t.receiver_user_id
  WHERE us.user_id IS NULL
     OR ur.user_id IS NULL
),

-- Event anomalies
evt_unknown_user AS (
  SELECT
    'app_events' AS table_name,
    'event_user_not_in_users' AS anomaly_type,
    COUNT(*) AS bad_rows
  FROM dab_bank.bronze_app_events e
  LEFT JOIN dab_bank.bronze_users u ON u.user_id = e.user_id
  WHERE e.user_id IS NOT NULL
    AND u.user_id IS NULL
)

-- Final UNION ALL
SELECT * FROM row_counts
UNION ALL SELECT * FROM txn_invalid_amount
UNION ALL SELECT * FROM txn_unknown_users
UNION ALL SELECT * FROM evt_unknown_user
ORDER BY 1, 2;