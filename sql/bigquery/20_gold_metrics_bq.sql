-- 20_gold_metrics_bq.sql
-- Gold layer for BigQuery: trusted aggregate views over silver layer.
-- Again, qualify names with project.dataset if needed.

CREATE OR REPLACE VIEW `gold_total_volume_transacted` AS
SELECT
  SUM(amount) AS total_volume_transacted
FROM `silver_transactions`
WHERE status = 'succeeded';

CREATE OR REPLACE VIEW `gold_daily_active_users` AS
SELECT
  DATE_TRUNC(DATE(event_ts), DAY) AS day,
  COUNT(DISTINCT user_id) AS dau
FROM `silver_app_events`
GROUP BY day;

CREATE OR REPLACE VIEW `gold_avg_transaction_size_per_user` AS
WITH per_user AS (
  SELECT
    sender_user_id AS user_id,
    AVG(amount) AS avg_txn_amount_sent
  FROM `silver_transactions`
  WHERE status = 'succeeded'
  GROUP BY user_id
)
SELECT
  AVG(avg_txn_amount_sent) AS avg_transaction_size_per_user
FROM per_user;

-- 20_gold_metrics.sql
-- Gold layer: trusted aggregates for BI/Product.
-- Using silver.* views as inputs.


CREATE OR REPLACE VIEW `dab_bank.gold_total_volume_transacted` AS
SELECT
  SUM(amount) AS total_volume_transacted
FROM `dab_bank.silver_transactions`
WHERE status = 'succeeded';

CREATE OR REPLACE VIEW `dab_bank.gold_daily_active_users` AS
SELECT
  DATE_TRUNC(DATE(event_ts), DAY) AS day,
  COUNT(DISTINCT user_id) AS dau
FROM `dab_bank.silver_app_events`
GROUP BY day;

CREATE OR REPLACE VIEW `dab_bank.gold_avg_transaction_size_per_user` AS
WITH per_user AS (
  SELECT
    sender_user_id AS user_id,
    AVG(amount) AS avg_txn_amount_sent
  FROM `dab_bank.silver_transactions`
  WHERE status = 'succeeded'
  GROUP BY user_id
)
SELECT
  AVG(avg_txn_amount_sent) AS avg_transaction_size_per_user
FROM per_user;

