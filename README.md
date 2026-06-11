# ⚽ Bangkok Football Intelligence Platform

End-to-end Data Engineering pipeline ที่ดึงข้อมูล Premier League จาก Football API
ผ่าน Airflow, ประมวลผลด้วย PySpark, เก็บใน Google Cloud Storage (Data Lake),
โหลดเข้า BigQuery และ transform ด้วย dbt พร้อม CI/CD ผ่าน GitHub Actions

## 🏗️ Architecture

```
[Football API (football-data.org)]
        │
        ▼
[Apache Airflow]  ── orchestration (daily schedule)
        │
        ▼
[GCS Raw Zone]    ── immutable raw JSON (partitioned: year=/month=/day=)
        │
        ▼
[PySpark Job]     ── flatten nested JSON, validate, derive columns
        │
        ▼
[GCS Processed Zone] ── Parquet (snappy, partitioned by year/month)
        │
        ▼
[BigQuery: football_staging.raw_matches]
        │
        ▼
[dbt]             ── staging → intermediate → mart
        │
        ▼
[BigQuery Mart: fct_matches, dim_teams]
        │
        ▼
[Looker Studio Dashboard]

────────────────────────────────────────────
[GitHub Actions CI/CD]
  • dbt compile + dbt test on every PR
  • Airflow DAG validation + flake8 lint
  • Auth via Workload Identity Federation (no JSON keys)
```

## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Orchestration | Apache Airflow 2.8 (Docker, LocalExecutor) |
| Processing | Apache Spark 3.5 (PySpark) |
| Data Lake | Google Cloud Storage (Raw + Processed Zones) |
| Warehouse | Google BigQuery (asia-southeast1) |
| Transformation | dbt-bigquery 1.7 |
| CI/CD | GitHub Actions + Workload Identity Federation |
| Infra | Docker Compose |
| Dashboard | Looker Studio |

## 📁 Project Structure

```
bangkok-football-intelligence/
├── docker-compose.yml          # Airflow + Spark + Postgres
├── Dockerfile.airflow          # Airflow image (+ Java, dbt, GCP libs)
├── requirements.txt
├── .github/workflows/
│   ├── dbt_ci.yml              # dbt compile + test on PR
│   └── dag_validation.yml      # DAG import check + lint
├── airflow/
│   └── dags/
│       └── ingest_matches_dag.py
├── ingestion/
│   └── football_api_client.py  # football-data.org client
├── spark/
│   ├── jobs/
│   │   └── clean_matches.py    # JSON → Parquet (flatten + validate)
│   └── jars/                   # GCS/BQ connectors (not committed)
└── football_intelligence/      # dbt project
    ├── dbt_project.yml
    ├── macros/generate_schema_name.sql
    ├── profiles/profiles.yml
    └── models/
        ├── staging/stg_matches.sql      # dedup + recast (view)
        ├── intermediate/int_match_results.sql  # points logic (table)
        └── mart/
            ├── fct_matches.sql          # match fact table
            └── dim_teams.sql            # league table dimension
```

## 🔄 Pipeline Flow (Airflow DAG: `ingest_football_matches`)

```
fetch_matches_from_api
  → upload_to_gcs (raw JSON)
  → download_raw_from_gcs
  → spark_clean_matches (PySpark, local[*])
  → upload_parquet_to_gcs
  → load_to_bigquery (WRITE_TRUNCATE → football_staging.raw_matches)
```

หลังจากนั้นรัน dbt:

```bash
dbt run    # stg_matches → int_match_results → fct_matches + dim_teams
dbt test   # 10 data quality tests (unique, not_null, accepted_values)
```

## 📊 dbt Layers

| Layer | Model | Materialization | หน้าที่ |
|---|---|---|---|
| Staging | `stg_matches` | view | rename, recast, **deduplicate** (ROW_NUMBER by match_id, latest ingested_at) |
| Intermediate | `int_match_results` | table | filter FINISHED, คำนวณ points (W=3/D=1/L=0), goal diff |
| Mart | `fct_matches` | table | one row per match |
| Mart | `dim_teams` | table | aggregated team stats + `league_position` (RANK by points, GD) |

## 🔐 Security Notes

- **ไม่มี Service Account JSON keys** — local dev ใช้ Application Default Credentials (ADC),
  CI ใช้ Workload Identity Federation
- Secrets (API key, bucket names) เก็บใน **Airflow Variables**

## 🚀 Quick Start

```bash
git clone https://github.com/custapq/football-pipeline.git
cd football-pipeline
cp .env.example .env          # ใส่ FOOTBALL_API_KEY และค่า GCP
gcloud auth application-default login
docker compose build
docker compose up airflow-init
docker compose up -d
# Airflow UI → http://localhost:8080 (admin/admin)
```

## 🧪 Sample Output

```
=== Premier League Table 2025/26 ===
1. Arsenal FC          — 85 pts (GD: +44)
2. Manchester City FC  — 78 pts (GD: +42)
3. Manchester United FC— 71 pts (GD: +19)
...
20. Wolverhampton FC   — 20 pts (GD: -41)
```
