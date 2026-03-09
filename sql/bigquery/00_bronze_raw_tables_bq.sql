


-- Users raw
CREATE OR REPLACE TABLE `dab_bank.bronze_users` (
  user_id STRING,
  signup_ts TIMESTAMP,
  country STRING,
  kyc_status STRING,
  device_os STRING,
  is_deleted BOOL,
  deleted_ts TIMESTAMP
);

-- Transactions raw
CREATE OR REPLACE TABLE `dab_bank.bronze_transactions` (
  transaction_id STRING,
  created_ts TIMESTAMP,
  sender_user_id STRING,
  receiver_user_id STRING,
  amount NUMERIC,
  currency STRING,
  status STRING,
  channel STRING
);

-- App events raw
CREATE OR REPLACE TABLE `dab_ank.bronze_app_events` (
  event_id STRING,
  event_ts TIMESTAMP,
  user_id STRING,
  event_type STRING,
  session_id STRING,
  page STRING,
  button_id STRING,
  platform STRING
);


