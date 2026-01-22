# 🚕 NYC Taxi Data Pipeline with Apache Airflow

<div align="center">

![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)
![Airflow](https://img.shields.io/badge/Apache%20Airflow-2.0+-red.svg)
![GCP](https://img.shields.io/badge/Google%20Cloud-Platform-4285F4.svg)
![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)

**Production-grade data pipeline orchestration demonstrating end-to-end ETL workflows**

[Demo Video](#-demo) • [Architecture](#-architecture) • [Getting Started](#-getting-started)

</div>

---

## 📋 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Key Features](#-key-features)
- [Tech Stack](#️-tech-stack)
- [Pipeline Workflow](#-pipeline-workflow)
- [Engineering Principles](#-engineering-principles)
- [Getting Started](#-getting-started)
- [Demo](#-demo)
- [Future Enhancements](#-future-enhancements)
- [Author](#-author)

---

## 🎯 Overview

This project showcases a **production-ready data pipeline** built with Apache Airflow, demonstrating how modern data engineering teams orchestrate ETL workflows at scale. Unlike one-off scripts, this pipeline is designed for **reliability, repeatability, and idempotency** — core requirements for enterprise data systems.

### What Makes This Production-Grade?

✅ **Scheduled & Automated** — No manual intervention required  
✅ **Idempotent Design** — Safe to re-run without data duplication  
✅ **Backfill-Ready** — Process historical data intervals seamlessly  
✅ **Fault-Tolerant** — Built-in retries and failure handling  
✅ **Scalable Architecture** — Separates data lake (storage) from data warehouse (analytics)

---

## 🏗️ Architecture

```
┌─────────────┐      ┌──────────────┐      ┌─────────────┐      ┌──────────────┐
│   NYC Taxi  │──────▶│   Apache     │──────▶│   Google    │──────▶│   BigQuery   │
│   Dataset   │      │   Airflow    │      │   Cloud     │      │  (Warehouse) │
│  (Source)   │      │ (Orchestrator)│      │  Storage    │      │              │
└─────────────┘      └──────────────┘      └─────────────┘      └──────────────┘
                            │                   (Data Lake)
                            │
                      ┌─────▼─────┐
                      │  Docker   │
                      │  Compose  │
                      └───────────┘
```

### Components

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Source** | NYC Taxi Public Dataset | Raw trip records |
| **Orchestration** | Apache Airflow (Dockerized) | Workflow scheduling & management |
| **Data Lake** | Google Cloud Storage (Parquet) | Scalable, low-cost storage for raw/semi-processed data |
| **Data Warehouse** | BigQuery | Analytics-optimized storage for reporting |

---

## ✨ Key Features

### 🔄 Workflow Orchestration
- **Task Dependencies** — Ensures correct execution order
- **Smart Scheduling** — Execution date-based data intervals
- **Automatic Retries** — Handles transient failures gracefully
- **Backfill Support** — Process historical data without code changes

### 🏢 Data Lake vs Data Warehouse Pattern
- **GCS (Data Lake)**: System of record for all raw and transformed data
- **BigQuery (Warehouse)**: Curated, structured data optimized for SQL analytics

### ♻️ Idempotent Pipeline Design
- Re-running tasks produces identical results
- No duplicate records on retry
- Safe recovery from partial failures
- Production-ready reliability

---

## 🛠️ Tech Stack

<table>
<tr>
<td><b>Language</b></td>
<td><img src="https://img.shields.io/badge/Python-3776AB?logo=python&logoColor=white" alt="Python"/></td>
</tr>
<tr>
<td><b>Orchestration</b></td>
<td><img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?logo=apache-airflow&logoColor=white" alt="Airflow"/></td>
</tr>
<tr>
<td><b>Cloud Platform</b></td>
<td><img src="https://img.shields.io/badge/Google%20Cloud-4285F4?logo=google-cloud&logoColor=white" alt="GCP"/></td>
</tr>
<tr>
<td><b>Containerization</b></td>
<td><img src="https://img.shields.io/badge/Docker-2496ED?logo=docker&logoColor=white" alt="Docker"/></td>
</tr>
<tr>
<td><b>Storage Format</b></td>
<td><img src="https://img.shields.io/badge/Apache%20Parquet-50ABF1?logo=apache&logoColor=white" alt="Parquet"/></td>
</tr>
</table>

---

## 📊 Pipeline Workflow

### Visual Flow

```
Extract Raw Data → Transform to Parquet → Load to GCS → Load to BigQuery
```

### Step-by-Step Process

1. **📥 Extract**
   - Download NYC Taxi data for specific execution date
   - Uses Airflow's logical date (not runtime) for correct data intervals

2. **🔧 Transform**
   - Convert raw CSV/JSON to optimized Parquet format
   - Improves storage efficiency and query performance

3. **💾 Load to Data Lake**
   - Store processed Parquet files in Google Cloud Storage
   - Preserves complete historical record

4. **📈 Load to Data Warehouse**
   - Load curated data into BigQuery tables
   - Enables fast SQL analytics and BI tool integration

---

## 🎓 Engineering Principles

### 1. **Execution Time vs Run Time**
Pipeline scheduling uses logical execution dates (data intervals) rather than wall-clock time, ensuring correct historical data processing during backfills.

### 2. **Separation of Concerns**
- **Data Lake**: Durable, cost-effective storage for all data states
- **Data Warehouse**: Fast, structured storage for analytics workloads

### 3. **Fault Tolerance**
- Configurable retry logic for transient failures
- Idempotent operations prevent data corruption
- Clear task boundaries for easy debugging

### 4. **Scalability**
- Dockerized deployment for consistent environments
- Cloud-native storage scales with data volume
- DAG design supports parallel task execution

---

## 🚀 Getting Started

### Prerequisites
- Docker & Docker Compose installed
- Google Cloud Platform account
- GCP service account with Storage & BigQuery permissions

### Setup

```bash
# Clone the repository
git clone https://github.com/corerage/Data-Engineering.git
cd Data-Engineering

# Configure GCP credentials
export GOOGLE_APPLICATION_CREDENTIALS=/path/to/service-account-key.json

# Start Airflow
docker-compose up -d

# Access Airflow UI
open http://localhost:8080
```

### Running the Pipeline

1. Navigate to Airflow UI (default: `admin/admin`)
2. Enable the `nyc_taxi_pipeline` DAG
3. Trigger manually or wait for scheduled run
4. Monitor task execution in Graph/Tree view

---

## 🎥 Demo

**13-Minute Walkthrough** covering:
- ✅ DAG structure and task dependencies
- ✅ Dockerized Airflow configuration
- ✅ Complete pipeline execution
- ✅ Data verification in GCS and BigQuery

📺 **[Watch Demo Video](#)** *((https://www.linkedin.com/posts/ebube-nwaigbo_dataengineering-apacheairflow-workfloworchestration-activity-7419412419615277056-L4rZ?utm_source=share&utm_medium=member_desktop&rcm=ACoAACZzc0AByHfqnyXIEv0pGikLBn1jw68dB50))*

---

## 🔮 Future Enhancements

- [ ] **Data Quality Checks** — Great Expectations integration for validation
- [ ] **Sensors** — Monitor upstream data availability before execution
- [ ] **Partitioned Backfills** — Optimize large-scale historical data processing
- [ ] **Monitoring & Alerting** — Slack/Email notifications for pipeline failures
- [ ] **dbt Integration** — Add transformation layer for complex analytics
- [ ] **CI/CD Pipeline** — Automated testing and deployment

---

## 👨‍💻 Author

**Courage Nwaigbo**  
*Data Engineer | Data Scientist*

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0077B5?logo=linkedin)](www.linkedin.com/in/ebube-nwaigbo)


---

<div align="center">

### ⭐ If you found this project helpful, please consider giving it a star!

**Built with ❤️ and ☕ by Courage Nwaigbo**

</div>