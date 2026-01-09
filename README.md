# Real-Time Car Park Analytics Pipeline

## 📖 Project Overview
This project implements a robust data engineering pipeline to monitor and analyze car park occupancy in New South Wales (NSW). Using the **Transport for NSW Car Park API**, the system ingests live data via **Kafka**, processes it with **Airflow**, and stores refined metrics in an **SQLite** database for daily reporting.

## 🏗 System Architecture
The pipeline is divided into three automated jobs:

### 1. Job 1: Ingestion (Real-Time)
* **Frequency:** Every 1 minute.
* **Logic:** Fetches facility data (e.g., TSN 2155384 for Tallawong) from the TfNSW API.
* **Output:** Produces raw JSON events to the `raw_events` Kafka topic.

### 2. Job 2: Cleaning & Storage
* **Frequency:** Hourly.
* **Logic:** Consumes Kafka messages and cleans data (handling nulls and type conversion).
* **Transformation:** Calculates availability using the formula: $Availability = spots - total$.
* **Storage:** Stores cleaned records in the `events` table in SQLite.

### 3. Job 3: Daily Analytics
* **Frequency:** Daily at 00:00.
* **Logic:** Aggregates daily occupancy trends.
* **Metrics:** Identifies peak hours, average utilization, and writes results to the `daily_summary` table.

---

## 🛠 Tech Stack
* **Orchestration:** Apache Airflow
* **Streaming:** Apache Kafka
* **Processing:** Python (Pandas)
* **Database:** SQLite

---
📊 Pipeline Workflow (DAGs)JobNameFrequencyResponsibility
Job 1 job1_ingestion_dagEvery 1 minPolls TfNSW API and produces messages to Kafka topic.
Job 2 job2_clean_store_dagHourlyConsumes Kafka, cleans data, and writes to SQLite events.
Job 3 job3_daily_summary_dagDaily @ 00:00Computes occupancy metrics and writes to daily_summary.

```bash
📂 Project StructurePlaintext.
├── airflow/
│   └── dags/               # Airflow DAG definitions
├── src/
│   ├── job1_producer.py    # Ingestion: API ➡️ Kafka
│   ├── job2_cleaner.py     # Processing: Kafka ➡️ SQLite
│   ├── job3_analytics.py   # Analytics: SQL Aggregation
│   └── db_utils.py         # Database helper functions
├── data/
│   └── app.db              # SQLite Database
└── requirements.txt        # Python dependencies
