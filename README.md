#  Banking Fraud Streaming Pipeline

![AWS](https://img.shields.io/badge/AWS-Kinesis%20%7C%20Glue%20%7C%20Lambda%20%7C%20Athena%20%7C%20S3%20%7C%20CloudWatch-orange)
![PySpark](https://img.shields.io/badge/PySpark-4.0-red)
![Python](https://img.shields.io/badge/Python-3.9-blue)
![Streaming](https://img.shields.io/badge/Streaming-Realtime-brightgreen)
![Dashboard](https://img.shields.io/badge/Dashboard-ApexCharts.js-brightgreen)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

A **real‑time streaming data pipeline** that detects fraudulent banking transactions as they happen.
The pipeline ingests transaction data from a simulated producer into **Amazon Kinesis**, processes it with **AWS Glue Streaming (PySpark)**, stores enriched Parquet files in **S3**, enables analytics via **Athena**, and serves an **auto‑refreshing static dashboard** — all serverless and automated.

---

## 📌 Business Problem

Banks lose millions to fraud every year. Traditional batch pipelines (e.g., hourly or daily) introduce latency, allowing fraud to go undetected until after the damage is done.

**This project solves that by building a real‑time streaming pipeline that:**
- Ingests transactions **immediately** as they occur (simulated via a Python producer).
- **Detects fraud** using rule‑based logic (e.g., `isFraud=1` or amount > 200,000).
- **Enriches and transforms** data, then writes it as **partitioned Parquet** into S3.
- **Updates a live dashboard** every 5 minutes (via Lambda + Athena) and auto‑refreshes every 30 seconds.
- Provides **real‑time visibility** into fraud patterns, suspicious accounts, and transaction volume.

---

## 🏗️ Architecture Overview

<img width="1402" height="740" alt="architecture" src="https://github.com/user-attachments/assets/ccd84843-5a49-4f3e-bed9-02de569d846d" />


*End‑to‑end serverless streaming pipeline for real‑time fraud detection.*

| Layer | Service | Role |
|-------|---------|------|
| **Ingestion** | Python Producer + Kinesis | Simulates transactions, publishes to Kinesis stream |
| **Stream Processing** | AWS Glue Streaming (PySpark) | Reads from Kinesis, enriches data, flags fraud, writes Parquet |
| **Storage** | S3 | Raw & processed data (partitioned Parquet) |
| **Analytics** | Amazon Athena | Serverless SQL queries on streaming data |
| **Summary Generation** | Lambda + CloudWatch Events | Runs Athena queries every 5 minutes, writes `fraud_summary.json` |
| **Visualization** | S3 Static Website + ApexCharts | Auto‑refreshing dashboard, pulls JSON from S3 |

> **Cost**: Fully serverless – runs under **$1–2 per month** using AWS Free Tier / Learner Lab credits.

---

## 📂 Repository Structure

```
Banking-Fraud-Streaming-Pipeline/
├── producer/
│   └── producer.py                  # Kinesis producer (simulates transactions with random fraud)
├── glue-script/
│   └── streaming_fraud_etl.py       # Glue Streaming job (PySpark)
├── lambda/
│   └── fraud_summary_generator.py   # Lambda to generate fraud summary JSON
├── dashboard/
│   └── index.html                   # Static dashboard (Tailwind + ApexCharts)
├── cloudformation/
│   └── kinesis-stream.yaml          # CloudFormation for Kinesis stream
├── docs/
│   └── architecture.png             # Architecture diagram

```

---

## 🛠️ Technologies & Services

| Category | Tools / Services |
|----------|------------------|
| **Cloud** | AWS (Kinesis, Glue, Lambda, Athena, S3, CloudWatch Events, CloudWatch Logs) |
| **Stream Processing** | PySpark (AWS Glue Streaming 4.0) |
| **Storage** | S3 (Parquet + Snappy compression, partitioned) |
| **Query Engine** | Amazon Athena |
| **Orchestration** | CloudWatch Events (triggers Lambda every 5 minutes) |
| **Dashboard** | HTML, Tailwind CSS, ApexCharts (static S3 website) |
| **Version Control** | Git, GitHub |

---

## 📊 Dataset & Fraud Simulation

- **Base data**: PaySim financial dataset (synthetic mobile money transactions).
- **Producer behavior**: Reads a sample CSV and publishes records to Kinesis.
- **Fraud injection**: 2% of records are randomly marked as `isFraud=1`.
- **Final fraud flag**: `is_fraud_final = 1` if `isFraud=1` **OR** transaction amount > $200,000.
- **Output format**: Parquet partitioned by `is_fraud_final` and `partition_date`.

---

## 🧠 How the Pipeline Works (Step‑by‑Step)

1. **Producer** → Reads CSV, adds random fraud (`isFraud=1` with 2% probability), publishes to **Kinesis** stream.
2. **Glue Streaming Job** → Consumes Kinesis stream in real time:
   - Parses JSON, adds `processing_timestamp`.
   - Applies fraud rules → creates `is_fraud_final`.
   - Writes enriched data to S3 as **Parquet** (partitioned by `is_fraud_final` and `partition_date`).
3. **Athena** → External table on the S3 Parquet files for SQL analytics.
4. **Lambda (scheduled)** → Triggered by CloudWatch Events every 5 minutes:
   - Runs Athena queries for summary stats, hourly fraud, and top accounts.
   - Writes results to `fraud_summary.json` in the **dashboard S3 bucket**.
5. **Static Dashboard** → Hosted on S3 static website:
   - Fetches `fraud_summary.json` every 30 seconds (auto‑refresh).
   - Displays KPI cards, hourly fraud chart, and top suspicious accounts.

---

## 🚀 Deployment Instructions (Manual)

### Prerequisites
- AWS account (Learner Lab or personal) with permissions for Kinesis, Glue, Lambda, Athena, S3, CloudWatch Events, IAM.
- AWS CLI installed (optional, you can use Console).

### Steps

**1. Create Kinesis stream**
Deploy `cloudformation/kinesis-stream.yaml` (or use AWS Console).
Stream name: `banking-transaction-stream`.

**2. Create S3 buckets** (if not already done)
- Processed bucket: `banking-processed-<suffix>/streaming-output/`
- Dashboard bucket: `banking-dashboard-<suffix>` (enable static website hosting)

**3. Upload Glue script**
- Upload `glue-script/streaming_fraud_etl.py` to your raw/scripts bucket.

**4. Create Glue Streaming Job** (via Glue Studio → Script editor)
- Job name: `banking-streaming-job`
- Type: **Spark Streaming**
- IAM role: `LabRole`
- Worker: G.1X (2 workers)
- Job parameters:
  - `--INPUT_STREAM_NAME` = `banking-transaction-stream`
  - `--OUTPUT_PATH` = `s3://banking-processed-<suffix>/streaming-output/`
  - `--CHECKPOINT_PATH` = `s3://banking-processed-<suffix>/checkpoint/`

**5. Create Lambda function** (Python 3.11)
- Name: `banking-streaming-fraud-summary`
- Use code from `lambda/fraud_summary_generator.py`.
- Timeout: 1 minute.
- IAM role: `LabRole`.

**6. Create CloudWatch Events rule**
- Schedule: `rate(5 minutes)`
- Target: Lambda `banking-streaming-fraud-summary`.

**7. Upload dashboard**
- Upload `dashboard/index.html` to your dashboard S3 bucket (root).

**8. Run producer** (on any machine with AWS credentials)

```bash
python producer/producer.py
```

**9. Monitor**

- Glue job should be **Running**; after a few minutes, check S3 for Parquet files.
- Lambda runs every 5 minutes → creates `fraud_summary.json`.
- Open the dashboard S3 website URL → auto‑refreshing dashboard shows live fraud stats.

---

## 📸 Screenshots

| # | What to Screenshot | Where to Find It |
|---|-------------------|-----------------|
| 1 | Architecture Diagram | `docs/architecture.png` – draw.io or Eraser.io diagram |
| 2 | Live Dashboard | S3 website endpoint – full page showing KPI cards, hourly fraud chart, top accounts table |
| 3 | Glue Streaming Job Log (success) | Glue Console → Jobs → `banking-streaming-job` → Runs → view logs (CloudWatch) |
| 4 | Athena Query Result | Athena Console → run `SELECT is_fraud_final, COUNT(*) FROM banking_db.streaming_fraud GROUP BY is_fraud_final;` |
| 5 | S3 Parquet Files (partitioned) | Processed bucket → `streaming-output/is_fraud_final=0/partition_date=...` showing Parquet files |
| 6 | CloudWatch Events Rule | CloudWatch Console → Events → Rules → `banking-fraud-refresh-5min` |
| 7 | Lambda Logs | Lambda Console → `banking-streaming-fraud-summary` → Monitor → View logs |
| 8 | JSON summary file | Dashboard bucket → `fraud_summary.json` – open to show JSON structure |

---

## 📈 Example Query & Results

```sql
SELECT is_fraud_final, COUNT(*) as cnt
FROM banking_db.streaming_fraud
GROUP BY is_fraud_final;
```

| is_fraud_final | cnt |
|----------------|-----|
| 0 | 17,186 |
| 1 | 9,783 |

Fraud rate: ~36% (due to high‑value transactions in the test sample).

---

## 📊 Dashboard Preview

<img width="1567" height="809" alt="dashboard" src="https://github.com/user-attachments/assets/d39b8173-b596-4693-9514-f84c764389f2" />


*Auto‑refreshing dashboard (30s interval) showing KPI cards, hourly fraud attempts, and top suspicious accounts.*

---

## 🔐 Security & Cost Optimization

- **Encryption**: SSE‑S3 enabled on all buckets.
- **IAM**: Least‑privilege roles (`LabRole` used in this implementation).
- **Partitioning**: Parquet data partitioned by `is_fraud_final` and `partition_date` → reduces Athena query cost.
- **Serverless**: No EC2 or always‑on services → costs near zero when idle.
- **Lifecycle policies**: (optional) set expiration for old streaming output.

---

## 🧠 Skills Demonstrated

| Skill | Evidence |
|-------|----------|
| Real‑time data ingestion | Kinesis + Python producer |
| Stream processing | AWS Glue Streaming (PySpark) |
| Data enrichment & transformation | Derived columns, fraud rules |
| Partitioned storage | Parquet + Snappy compression, Hive partitioning |
| Serverless orchestration | CloudWatch Events (scheduled Lambda) |
| Analytics & querying | Athena external tables |
| Dashboard & visualization | Static S3 website + ApexCharts + auto‑refresh |
| Infrastructure as Code | CloudFormation (Kinesis stream) |

---

## 🔮 Future Improvements

- Add real‑time ML inference (e.g., SageMaker endpoint called from Glue job).
- Replace Lambda with Kinesis Data Analytics for lower latency.
- Add SNS alerts when fraud rate exceeds a threshold.
- Implement exactly‑once semantics and state management (using Glue's checkpointing).

---

## 🤖 AI Use Declaration

During the development of this project, AI tools were used for:

- Language translation and sentence refinement
- Code suggestions, debugging, and structural guidance
- Writing assistance for the README and documentation
- Brainstorming and conceptual support

However, all core architectural decisions, data modeling, feature engineering, pipeline configuration, result interpretation, and final technical validations were performed by the author (Paradorn Khanongsuwan). All AI‑generated outputs have been manually verified and adapted.

---

## 📜 Citation

If you use this project, code, or architecture in your work, please cite:

```bibtex
@misc{khanongsuwan_2026_banking_fraud_streaming,
  title={Banking Fraud Streaming Pipeline – Real‑time AWS ETL with Live Dashboard},
  author={Khanongsuwan, Paradorn},
  year={2026},
  howpublished={\url{https://github.com/iHazelly/Banking-Fraud-Streaming-Pipeline}}
}
```

---

## 🙏 Acknowledgements

- **Dataset**: [PaySim Financial Dataset (Kaggle)](https://www.kaggle.com/datasets/ealaxi/paysim1)
- **AWS Learner Lab** – cloud credits for hands‑on learning
- **AIT Information Management** – academic guidance
- **Open‑source libraries**: Apache Spark, boto3, ApexCharts, Tailwind CSS

---

## 📬 Contact

- **GitHub**: [github.com/iHazelly](https://github.com/iHazelly)
- **Project Repo**: [Banking-Fraud-Streaming-Pipeline](https://github.com/iHazelly/Banking-Fraud-Streaming-Pipeline)

Feel free to open an issue or pull request for improvements!

---

## ✅ Summary

This project is a complete, production‑grade streaming pipeline that demonstrates every stage of modern data engineering – from real‑time ingestion to interactive dashboards – on AWS. It is designed to be reproducible, cost‑efficient, and portfolio‑ready, perfect for showcasing skills required for roles like **Data Engineer**, **Streaming Engineer**, or **Data Platform Engineer**, especially in the banking/finance domain.

---

> 🎯 *"From live transaction stream to fraud detection dashboard – fully serverless, real‑time, and ready for production."*
