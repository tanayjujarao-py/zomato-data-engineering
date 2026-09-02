# 🍽️ Zomato Batch Data Pipeline

### End-to-End Batch ELT Pipeline — S3 → Snowflake → dbt → Airflow

A production-style batch data pipeline that takes Zomato-style food delivery data from raw CSVs in a data lake all the way to business-ready analytics tables — fully orchestrated and scheduled to run daily.

```
📂 Raw CSVs → ☁️ Amazon S3 → ❄️ Snowflake → 🔧 dbt → 🌀 Airflow
```

---

## 📌 Overview

This project simulates a real-world food delivery company's data stack. Raw dimension and fact data (restaurants, users, orders, reviews, etc.) lands in an S3 data lake, flows into Snowflake through a **keyless storage integration**, and is transformed through a **medallion architecture (Bronze → Silver → Gold)** using dbt. **Apache Airflow** orchestrates the entire pipeline as a single daily DAG — with no manual steps required to keep the warehouse up to date.

---

## 🏗️ Architecture

| Layer | Where | What Happens |
|---|---|---|
| 📂 **Source** | `data/` (local) | 4 dimension CSVs (restaurants, users, food, menu) + 3 fact files (10M orders, ~23M order items, 300K reviews) |
| ☁️ **Lake** | Amazon S3 | One bucket, `raw/<table>/` folder per source file |
| 🥉 **Bronze** | Snowflake `ZOMATO.RAW` | Loaded via `COPY INTO` from S3 through a keyless storage integration |
| 🥈 **Silver** | Snowflake `ZOMATO.STAGING` | dbt staging views — cleaned, typed, renamed columns |
| 🥇 **Gold** | Snowflake `ZOMATO.MARTS` | Dimensions, incremental fact tables (`MERGE`), business-ready marts + SCD2 snapshot |
| 🌀 **Orchestration** | Airflow (Docker) | One daily DAG — reload raw data → transform via dbt |

---

## 🛠️ Tech Stack

| Category | Tools |
|---|---|
| **Data Lake & Warehouse** | Amazon S3 · Snowflake |
| **Transformation** | dbt (dbt-snowflake) |
| **Orchestration** | Apache Airflow 3 (Docker) |
| **Language** | Python · SQL |
| **Infra & Security** | AWS IAM · Snowflake Storage Integration |

---

## 📁 Repository Structure

```
├── airflow/                     # Airflow 3 on Docker
│   ├── Dockerfile               #   Snowflake provider, dbt in its own venv
│   ├── docker-compose.yaml      #   postgres + api-server + scheduler
│   ├── example.env              #   template for SNOWFLAKE_* credentials
│   └── dags/zomato_batch.py     #   the pipeline DAG (2 tasks)
│
├── zomato/                      # dbt project
│   ├── models/staging/          #   staging views (Silver) + sources + tests
│   ├── models/marts/            #   dims, incremental facts, business marts (Gold)
│   └── macros/                  #   custom schema-name macro
│
├── snowflake/                   # Snowflake setup SQL (run in Snowsight, in order)
│   ├── 01_setup.sql             #   warehouse, database, schemas, role
│   ├── 02_storage_integration.sql  # keyless S3 ↔ Snowflake link
│   ├── 03_stage_and_formats.sql    # external stage + CSV file format
│   ├── 04_raw_tables.sql        #   RAW (Bronze) table DDL
│   └── 05_copy_into.sql         #   COPY INTO RAW from the stage
│
├── aws/iam/                     # IAM policy + trust policies for S3 ↔ Snowflake
└── docs/architecture.png        # architecture diagram
```

> `data/` (~2.3 GB of CSVs) and dbt `target/` artifacts are intentionally not committed to the repo due to size.

---

## ⚙️ How the Pipeline Works

### 1️⃣ Data Lands in S3
Seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/` — one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2️⃣ S3 → Snowflake: Keyless Handshake
Snowflake reads the S3 bucket with **no stored credentials**, using a Storage Integration + an IAM role.

**Setup order matters:**
1. Create the AWS IAM policy + role
2. Create the Snowflake `STORAGE INTEGRATION` pointing at the role ARN
3. Run `DESC INTEGRATION` to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID`
4. Paste both values into the IAM role's trust policy

> ⚠️ **Lesson learned:** The trust policy's `Principal` must be Snowflake's IAM user ARN, not `:root`. Also — never run `CREATE OR REPLACE` on the integration afterward, it regenerates the external ID and silently breaks the trust relationship.

### 3️⃣ Load — `COPY INTO`
Table DDL matches each CSV's exact column order. `COPY INTO` pulls each file from the external stage into `ZOMATO.RAW` tables — loading **10M orders, ~23M order items, and 300K reviews**.

### 4️⃣ Transform — dbt (Medallion Architecture)
- **Staging (Silver):** One view per source — parsing messy fields (e.g. `--` → `null`, `₹ 200` → `200`), lowercasing emails, deriving flags like `is_delivered`
- **Dimensions (Gold):** `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, a generated `dim_date` calendar
- **Facts (Gold, incremental):** `fct_orders` and `fact_order_items` use `materialized='incremental'` with a **MERGE strategy** — a re-run processes only new rows instead of rebuilding 10M+ records
- **Marts (Gold):** One table per business question — daily city revenue (GMV/AOV/cancel rate), restaurant performance, delivery SLA (p50/p90 by city & hour), review insights
- **Tests:** `unique` / `not_null` / `relationships` / `accepted_values` + a singular reconciliation test — `dbt build` runs models and tests together in dependency order

### 5️⃣ Orchestrate — Airflow
One daily DAG, `zomato_batch`, runs the full pipeline as a two-task graph:

```
reload_raw  →  dbt_build_code
(COPY INTO from S3)   (dbt build + tests)
```

- **`reload_raw`** — `SQLExecuteQueryOperator` runs the full `COPY INTO` batch against Snowflake, split into individual statements
- **`dbt_build_code`** — `BashOperator` runs `dbt build` inside the project's isolated virtual environment, executing every model and test in dependency order

🔐 **Credentials never touch the code** — `docker-compose` injects `SNOWFLAKE_*` environment variables (read by dbt's `profiles.yml` via `env_var()`) and an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the COPY task.

---

## 🚀 Getting Started

```bash
# 1. Clone the repository
git clone <repo-url>
cd zomato-batch-pipeline

# 2. Set up Snowflake (run in order, in Snowsight)
snowflake/01_setup.sql
snowflake/02_storage_integration.sql
snowflake/03_stage_and_formats.sql
snowflake/04_raw_tables.sql
snowflake/05_copy_into.sql

# 3. Configure environment variables
cp airflow/example.env airflow/.env
# fill in your SNOWFLAKE_* credentials

# 4. Spin up Airflow
cd airflow
docker-compose up

# 5. Trigger the DAG
# Open the Airflow UI → enable and trigger "zomato_batch"
```

---

## 🧠 Skills Demonstrated

| Domain | Skills |
|---|---|
| ☁️ **Cloud Data Engineering** | S3 data lake design, keyless cloud integration (IAM + Storage Integration), secure credential management |
| ❄️ **Data Warehousing** | Snowflake architecture, external stages, file formats, `COPY INTO` bulk loading |
| 🔧 **Data Transformation (dbt)** | Medallion architecture, staging/dimension/fact/mart modeling, incremental models with `MERGE` strategy, SCD Type 2 snapshots, custom macros, dbt testing |
| 🌀 **Orchestration** | Airflow 3 (Docker), DAG design, `SQLExecuteQueryOperator`, `BashOperator`, task dependencies, daily scheduling |
| 🐍 **Python & SQL** | Pipeline scripting, advanced SQL (joins, incremental logic, reconciliation tests) |
| 🔐 **Infrastructure & Security** | IAM policy design, environment-based secrets management, `.gitignore` practices for credentials |

---

## 📊 Scale

- **10M+** orders processed
- **~23M** order items
- **300K** customer reviews
- **7** source tables across Bronze/Silver/Gold layers
- **1** fully automated daily pipeline run — zero manual steps

---

## 🗺️ Roadmap / Next Steps

- [ ] Add AI-powered enrichment layer (LLM review analysis, RAG, text-to-SQL)
- [ ] Add CI/CD with GitHub Actions to run `dbt build` on every pull request
- [ ] Add Slack/email alerting on DAG failure

---

## 👤 Author

**Tanay Jujarao**
📧 tanayjujarao@gmail.com · 🔗 [LinkedIn](https://linkedin.com/in/tanay-jujarao) · 💻 [GitHub](https://github.com/tanayjujarao-py) · 🌐 [Portfolio](https://tanayjujarao-py.github.io)
