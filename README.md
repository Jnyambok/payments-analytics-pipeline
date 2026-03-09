# Payments Analytics Engineering Pipeline

A complete end-to-end analytics engineering project simulating one month of payments data across 1,000 users, 5,000 transactions, and 10,000 app events.
**Stack:** Python · SQL · BigQuery · Medallion Architecture

A complete end-to-end analytics engineering project simulating one month of payments data across 1,000 users, 5,000 transactions, and 10,000 app events. The pipeline covers synthetic data generation with intentional real-world anomalies, SQL-based anomaly detection and cleaning, a Medallion architecture implementation on BigQuery, and an executive presentation with insights and a feature scaling recommendation.

---

## Project Structure

```
payments-analytics-pipeline/
├── data/                          # Generated CSV datasets (gitignored)
│   ├── users.csv
│   ├── transactions.csv
│   └── app_events.csv
├── sql/
│   ├── 01_identify_anomalies.sql  # Bronze layer anomaly detection
│   ├── 02_clean_datasets.sql      # Silver layer cleaning logic
│   └── 03_metrics.sql             # Gold layer trusted metrics
├── src/
│   ├── generate_data.py           # Parameterised data generation script
│   └── build_zip.py               # Deliverable bundler
├── presentation/
│   ├── deck.pptx                  # Executive slide deck
│   └── charts/                    # Auto-generated visualisations
├── project_brief.pdf              # Project scope and requirements
└── README.md
```

---

## Quickstart

### 1. Setup

```bash
python -m venv .venv
```

**Windows PowerShell:**
```powershell
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

**Mac/Linux:**
```bash
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Generate Data

Run the generation script with a seed and target month:

```bash
python src/generate_data.py --seed 42 --month 2026-01
```

This outputs:
- `data/users.csv`
- `data/transactions.csv`
- `data/app_events.csv`
- `presentation/charts/*.png` (auto-generated visualisations)

Change `--month` to any `YYYY-MM` value to simulate a different reporting period.

### 3. Load into BigQuery

Load the three CSVs as raw Bronze tables:

```
bronze_users
bronze_transactions
bronze_app_events
```

Use your preferred method: BigQuery console upload, `bq load` CLI, or the BigQuery Python client.

### 4. Run the SQL Scripts

Execute in order:

```
sql/01_identify_anomalies.sql   -- surfaces all bad data from Bronze
sql/02_clean_datasets.sql       -- returns clean Silver datasets
sql/03_metrics.sql              -- computes trusted Gold metrics
```

All SQL is written to BigQuery dialect. Minor adjustments may be needed for other engines (Snowflake, Redshift, DuckDB).

---

## Architecture

This project follows a Medallion architecture pattern, the industry standard for scalable analytics engineering:

```
Bronze (Raw)  -->  Silver (Clean)  -->  Gold (Metrics)
```

| Layer | Tables | Purpose |
|-------|--------|---------|
| Bronze | `bronze_users`, `bronze_transactions`, `bronze_app_events` | Raw ingestion, no transforms. Single source of truth and audit record. |
| Silver | `silver_users`, `silver_transactions`, `silver_app_events` | Deduplicated, validated, referentially and temporally clean datasets. |
| Gold | `gold_total_volume`, `gold_daily_active_users`, `gold_avg_txn_size` | Trusted aggregates ready for BI and executive reporting. |

---

## Anomalies Introduced

The dataset intentionally injects three real-world anomaly classes to stress-test the pipeline:

### 1. Invalid Payment Amounts
Transactions with amounts at or below zero, and extreme outliers well above realistic consumer transfer ranges.

**Why it happens in production:** Currency conversion bugs, mis-configured API payloads, or test transactions that leak into production data.

**How the pipeline handles it:** Silver layer applies a range check, excluding amounts outside the valid business boundary.

### 2. Referential Integrity Violations
Some transactions reference a receiver ID that does not exist in the users table. Some app events reference a user ID with no corresponding user record.

**Why it happens in production:** Race conditions during account creation, partial data migrations, or client-side identity binding failures.

**How the pipeline handles it:** Silver layer enforces JOIN validation on both the sender and receiver side of every transaction, and on the user side of every event.

### 3. Temporal and Pipeline Window Issues
A subset of transactions carry timestamps just outside the reporting month window, though still after the relevant user's signup date.

**Why it happens in production:** Late-arriving data, timezone mismatches between microservices, or ingestion pipeline clock drift.

**How the pipeline handles it:** Silver layer applies a temporal integrity check, ensuring transaction timestamps are within the defined reporting window and after both parties' signup dates.

---

## Metrics Computed

All metrics are computed from the Silver (clean) layer only:

| Metric | Logic |
|--------|-------|
| Total Volume Transacted | `SUM(amount)` on succeeded transactions only |
| Daily Active Users (DAU) | `COUNT(DISTINCT user_id)` per day from clean events |
| Average Transaction Size per User | Per-user `AVG(amount)`, then averaged across users to avoid heavy-sender distortion |

---

## Key Findings

- DAU remained stable throughout the month with natural weekday/weekend variation, indicating genuine feature adoption rather than launch-spike behaviour
- Daily transaction volume grew consistently with modest spikes correlating with likely pay cycle dates
- Transaction amount distribution was right-skewed, with the majority of transfers falling within a typical consumer range and a long tail of larger transfers -- consistent with real-world P2P behaviour

---

## Strategic Recommendation

Scale cautiously, with a strong emphasis on data quality first.

The engagement and volume patterns indicate the feature is delivering consistent value. However, the anomaly analysis demonstrates how easily bad data can distort KPIs. Before significant investment in marketing or geographic expansion, the recommended next steps are:

1. **Harden validation at ingestion** -- implement range checks and referential constraints at the API and event tracking layer
2. **Formalise the Medallion model** -- document Bronze/Silver/Gold SLAs and enforce them as part of the data engineering workflow
3. **Build a payments health dashboard** -- give product and data teams shared visibility into volume, DAU, and anomaly rates in real time

---

## Requirements

```
pandas
numpy
faker
matplotlib
python-pptx
```

Install with:
```bash
pip install -r requirements.txt
```

---

## Bundling Deliverables

To create a zip of all deliverables:

```powershell
python src/build_zip.py
```

This produces `deliverable.zip` in the repo root.

---

## Notes

- SQL is BigQuery-friendly and largely ANSI-compatible
- The `--seed` flag ensures fully reproducible dataset generation
- Bronze tables are never modified -- all cleaning happens in Silver views
- The executive presentation was built to be accessible to both technical and non-technical audiences
