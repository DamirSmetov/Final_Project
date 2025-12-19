# Real-Time Car Park Analytics Pipeline

## 📖 Project Overview
This project implements a robust data engineering pipeline to monitor and analyze car park occupancy in New South Wales (NSW). Using the **Transport for NSW Car Park API**, the system ingests live data via **Kafka**, processes it with **Airflow**, and stores refined metrics in an **SQLite** database for daily reporting.

## 👥 Team Members
1. **Smetov Damir**
2. **Ravshanbekov Assadbek**
3. **Yerken Yarmukhamed**

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
* **Environment:** GitHub Codespaces

## 🚀 Execution Instructions
1.  **Environment:** Open in GitHub Codespaces.
2.  **Services:** Start Zookeeper and Kafka broker.
3.  **Airflow:** Run `airflow standalone` or start the scheduler/webserver separately.
4.  **DAGs:** Unpause `job1_ingestion_dag`, `job2_clean_store_dag`, and `job3_daily_summary_dag`.

---


## 🚀 Installation & Quick Start
**1. Install Dependencies**
```bash
brew install kafka zookeeper
pip install -r requirements.txt
2. Start ServicesTerminal 1: zookeeper-server-start /usr/local/etc/kafka/zookeeper.propertiesTerminal
2: kafka-server-start /usr/local/etc/kafka/server.properties3. Setup AirflowBashexport AIRFLOW_HOME=$(pwd)/airflow
airflow standalone
🐧 For Linux (Ubuntu)
1. Install Java & KafkaBashsudo apt update && sudo apt install default-jdk -y
# Download Kafka from official site, extract and cd into the folder
2. Start ServicesTerminal 1: bin/zookeeper-server-start.sh config/zookeeper.propertiesTerminal
2: bin/kafka-server-start.sh config/server.properties3. Setup AirflowBashexport AIRFLOW_HOME=$(pwd)/airflow
pip install apache-airflow pandas kafka-python
airflow db init
airflow standalone
```

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