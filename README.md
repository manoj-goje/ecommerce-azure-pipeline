# 🛒 E-Commerce Sales Analytics Pipeline
### End-to-End Azure Data Engineering | Medallion Architecture | Databricks + Delta Lake

![Azure](https://img.shields.io/badge/Azure-Data_Factory-0078D4?style=flat&logo=microsoft-azure)
![Databricks](https://img.shields.io/badge/Databricks-PySpark-FF3621?style=flat&logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-003366?style=flat)
![Synapse](https://img.shields.io/badge/Azure-Synapse_Analytics-0078D4?style=flat&logo=microsoft-azure)
![CI/CD](https://img.shields.io/badge/GitHub_Actions-CI%2FCD-2088FF?style=flat&logo=github-actions)

---

## 📌 Project Overview

A production-grade batch data pipeline that ingests raw e-commerce transaction data (orders, customers, products) from CSV/JSON sources, processes it through a **Medallion Architecture (Bronze → Silver → Gold)** on **Azure Databricks**, and serves BI-ready KPI views via **Azure Synapse Analytics**.

The entire pipeline is automated using **GitHub Actions CI/CD**, deploying notebooks from `develop` → `main` across Dev and Prod Databricks workspaces.

---

## 🏗️ Architecture

```
Raw Data Sources         Ingestion            Storage & Processing         BI Layer
─────────────────        ─────────            ────────────────────         ────────
 CSV  (Orders)  ──┐                           ┌─ Bronze (ADLS Gen2) ─┐
 CSV  (Customers)─┼──► Azure Data Factory ───►│  Silver (Databricks)  │──► Synapse Analytics
 JSON (Products) ─┘                           └─ Gold   (Delta Lake) ─┘     └──► Power BI
                             ▲
                    GitHub Actions CI/CD
```

### Medallion Layers

| Layer | Location | Description |
|-------|----------|-------------|
| 🥉 **Bronze** | ADLS Gen2 / Delta | Raw ingestion, schema-on-read, full history retained |
| 🥈 **Silver** | Databricks / Delta | Cleaned, deduplicated, type-cast, enriched |
| 🥇 **Gold** | Databricks / Delta | Aggregated KPI tables, BI-ready, partitioned + Z-ordered |

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|-----------|
| Cloud Platform | Microsoft Azure |
| Ingestion | Azure Data Factory (ADF) |
| Storage | Azure Data Lake Storage Gen2 (ADLS Gen2) |
| Processing | Azure Databricks, Apache Spark, PySpark |
| Table Format | Delta Lake (ACID, schema evolution, time travel) |
| BI / Serving | Azure Synapse Analytics (Serverless SQL Pool) |
| CI/CD | GitHub Actions |
| Language | Python, SQL |
| Data Formats | CSV, JSON, Parquet, Delta |

---

## 📁 Project Structure

```
ecommerce-pipeline/
│
├── notebooks/
│   ├── 01_bronze_ingestion.py       # Raw data → ADLS Gen2 Delta (Bronze)
│   ├── 02_silver_transformation.py  # Clean, deduplicate, enrich (Silver)
│   └── 03_gold_aggregation.py       # KPI aggregations, Z-ordering (Gold)
│
├── scripts/
│   └── 04_synapse_views.sql         # Synapse Serverless SQL views for BI
│
├── adf_pipelines/
│   └── deploy_pipeline.yml          # GitHub Actions CI/CD workflow
│
├── data/
│   └── sample/
│       └── orders_sample.csv        # Sample data for local testing
│
└── README.md
```

---

## 🚀 Pipeline Walkthrough

### Step 1 — Bronze: Raw Ingestion
**File:** `notebooks/01_bronze_ingestion.py`

- Mounts ADLS Gen2 using Service Principal + Azure Key Vault secrets
- Reads raw Orders (CSV), Customers (CSV), Products (JSON) from the raw container
- Adds audit columns: `_ingestion_timestamp`, `_source_file`, `_source_name`
- Writes to Delta Lake in **append mode** — preserving full raw history

### Step 2 — Silver: Transformation
**File:** `notebooks/02_silver_transformation.py`

Key transformations applied:
- **Orders:** Drop nulls on critical fields, dedup on `order_id`, parse dates, derive `order_year`/`order_month`, flag high-value orders (>₹1000)
- **Customers:** Lowercase + trim emails, normalise phone numbers, default `country_code` to `IN`
- **Products:** Standardise category names to UPPER, cast price to double, assign `price_tier` (BUDGET / MID / PREMIUM)
- Writes in **overwrite mode** with schema evolution enabled

### Step 3 — Gold: Aggregation
**File:** `notebooks/03_gold_aggregation.py`

Produces three Gold tables:

| Table | Description |
|-------|-------------|
| `monthly_revenue` | Revenue, orders, AOV per month — partitioned by year/month |
| `top_products` | Top 50 products by revenue with rank — partitioned by category |
| `customer_clv` | Customer Lifetime Value with PLATINUM/GOLD/SILVER/BRONZE segments |

Each table is **Z-ordered** for fast BI query performance.

### Step 4 — Synapse Analytics Views
**File:** `scripts/04_synapse_views.sql`

Creates four SQL views in Synapse Serverless Pool reading directly from Gold Delta tables via OPENROWSET:
- `gold.vw_monthly_revenue` — with MoM growth %
- `gold.vw_top_products` — top 10 by revenue
- `gold.vw_clv_summary` — segment breakdown
- `gold.vw_kpi_summary` — executive single-row KPI feed

---

## ⚙️ Setup & Prerequisites

### Azure Resources Required
- Azure Data Factory (with linked services to ADLS Gen2)
- Azure Data Lake Storage Gen2 (containers: `ecommerce-raw`, `ecommerce-bronze`, `ecommerce-silver`, `ecommerce-gold`)
- Azure Databricks workspace (with cluster running DBR 13.x+)
- Azure Synapse Analytics workspace (Serverless SQL Pool)
- Azure Key Vault (storing Service Principal credentials)

### GitHub Secrets Required

| Secret | Description |
|--------|-------------|
| `DATABRICKS_HOST` | Your Databricks workspace URL |
| `DATABRICKS_TOKEN` | Databricks Personal Access Token |
| `DEV_CLUSTER_ID` | Dev cluster ID for smoke tests |
| `DEV_STORAGE_ACCOUNT` | Dev ADLS Gen2 account name |

### Running Locally (Dev)
```bash
# Clone repo
git clone https://github.com/manoj-goje/ecommerce-pipeline.git
cd ecommerce-pipeline

# Upload notebooks to Databricks manually or push to `develop` branch
# GitHub Actions will auto-deploy to your Databricks workspace
git checkout develop
git push origin develop
```

---

## 📊 Gold Layer KPIs (Sample Output)

| Metric | Value |
|--------|-------|
| Total Revenue (All Time) | ₹48,32,109 |
| Total Orders | 12,847 |
| Unique Customers | 3,421 |
| Avg Order Value | ₹3,760 |
| Top Product Category | ELECTRONICS |

> *Sample values for illustration. Actual values depend on ingested data.*

---

## 🔐 Security Best Practices

- All credentials stored in **Azure Key Vault** — never hardcoded
- ADLS Gen2 accessed via **Service Principal + OAuth2** (not storage keys)
- Databricks secrets scoped via **`dbutils.secrets.get()`**
- GitHub Actions uses **repository secrets** — never plaintext tokens

---

## 📈 Performance Optimisations

- **Partitioning** on `order_year` / `order_month` / `category` — prunes irrelevant files at query time
- **Z-Ordering** on high-cardinality filter columns — co-locates related data in files
- **Delta Lake OPTIMIZE** run post-write on all Gold tables
- **Schema evolution** enabled on Silver writes — handles upstream changes gracefully

---

## 🏅 Certifications

- ✅ Microsoft Certified: **Azure Data Engineer Associate (DP-203)**
- ✅ **Databricks Certified Data Engineer Associate**

---

## 👤 Author

**Manoj Goje**
Azure Data Engineer | Pune, Maharashtra

[![LinkedIn](https://img.shields.io/badge/LinkedIn-manoj--goje-0A66C2?style=flat&logo=linkedin)](https://linkedin.com/in/manoj-goje)
[![GitHub](https://img.shields.io/badge/GitHub-manoj--goje-181717?style=flat&logo=github)](https://github.com/manoj-goje)

---

*Feel free to ⭐ this repo if you found it useful!*
