# Productionization Guide — Annie's Magic Numbers Pipeline
# Databricks Medallion Architecture

> **Goal:** Move the pipeline from a manual notebook run to a scheduled, monitored, production-grade Databricks Workflow.

---

## Table of Contents

1. [Unity Catalog Setup](#1-unity-catalog-setup)
2. [Upload Source Files to a Managed Volume](#2-upload-source-files-to-a-managed-volume)
3. [Create a Databricks Workflow (Job)](#3-create-a-databricks-workflow-job)
4. [Cluster Configuration](#4-cluster-configuration)
5. [Scheduling & Triggering](#5-scheduling--triggering)
6. [Delta Table Optimization](#6-delta-table-optimization)
7. [Monitoring & Alerting](#7-monitoring--alerting)
8. [CI/CD with Databricks Asset Bundles](#8-cicd-with-databricks-asset-bundles)
9. [Access Control & Row-Level Security](#9-access-control--row-level-security)
10. [Estimated Compute Costs](#10-estimated-compute-costs)

---

## 1. Unity Catalog Setup

Unity Catalog provides centralized access control, data lineage, and auditing across all Delta tables.

### Create the Catalog and Schemas

```sql
-- Run in a Databricks SQL Warehouse or notebook
CREATE CATALOG IF NOT EXISTS annie_magic_numbers
  COMMENT 'Annie''s liquor distributor profitability pipeline';

USE CATALOG annie_magic_numbers;

CREATE SCHEMA IF NOT EXISTS bronze COMMENT 'Raw ingested CSV data';
CREATE SCHEMA IF NOT EXISTS silver COMMENT 'Cleaned and enriched data';
CREATE SCHEMA IF NOT EXISTS gold   COMMENT 'Analytical tables for Power BI';
```

> **Update the notebooks:** Change `CATALOG = "main"` to `CATALOG = "annie_magic_numbers"` in all 3 notebooks.

### Grant Access to Power BI Service Principal

```sql
-- Create a service principal for Power BI
CREATE SERVICE CREDENTIAL IF NOT EXISTS powerbi_sp ...;

-- Grant read-only access to Gold tables
GRANT USE CATALOG ON CATALOG annie_magic_numbers TO `powerbi-service-principal`;
GRANT USE SCHEMA   ON SCHEMA annie_magic_numbers.gold TO `powerbi-service-principal`;
GRANT SELECT       ON SCHEMA annie_magic_numbers.gold TO `powerbi-service-principal`;
```

---

## 2. Upload Source Files to a Managed Volume

Using Unity Catalog **Volumes** (recommended over legacy DBFS) for governed file storage.

```sql
-- Create a volume to store raw source files
CREATE VOLUME IF NOT EXISTS annie_magic_numbers.bronze.raw_files
  COMMENT 'Raw CSV source files from Annie''s distributor system';
```

Upload via Databricks CLI:
```bash
pip install databricks-cli
databricks configure --token          # enter your workspace URL + PAT token

# Upload all 6 CSV files
databricks fs cp ./files/ /Volumes/annie_magic_numbers/bronze/raw_files/ --recursive
```

Update `DBFS_ROOT` in `01_bronze_ingestion.py`:
```python
DBFS_ROOT = "/Volumes/annie_magic_numbers/bronze/raw_files"
```

---

## 3. Create a Databricks Workflow (Job)

A Workflow chains the 3 notebooks into a single orchestrated job with dependency management.

### Steps in the UI

1. Go to **Workflows** in the left sidebar
2. Click **+ Create Job**
3. Name it: `Annie Magic Numbers — ETL Pipeline`
4. Add **Task 1: Bronze Ingestion**
   - Task name: `bronze_ingestion`
   - Type: `Notebook`
   - Source: Workspace path to `01_bronze_ingestion.py`
   - Cluster: (see Section 4)
5. Add **Task 2: Silver Cleaning**
   - Task name: `silver_cleaning`
   - Depends on: `bronze_ingestion`
   - Source: `02_silver_cleaning.py`
6. Add **Task 3: Gold Metrics**
   - Task name: `gold_metrics`
   - Depends on: `silver_cleaning`
   - Source: `03_gold_metrics.py`

### Equivalent JSON (for programmatic creation via REST API)

```json
{
  "name": "Annie Magic Numbers — ETL Pipeline",
  "tasks": [
    {
      "task_key": "bronze_ingestion",
      "notebook_task": { "notebook_path": "/Repos/annie-pipeline/databricks/01_bronze_ingestion" },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "silver_cleaning",
      "depends_on": [{ "task_key": "bronze_ingestion" }],
      "notebook_task": { "notebook_path": "/Repos/annie-pipeline/databricks/02_silver_cleaning" },
      "job_cluster_key": "etl_cluster"
    },
    {
      "task_key": "gold_metrics",
      "depends_on": [{ "task_key": "silver_cleaning" }],
      "notebook_task": { "notebook_path": "/Repos/annie-pipeline/databricks/03_gold_metrics" },
      "job_cluster_key": "etl_cluster"
    }
  ],
  "job_clusters": [
    {
      "job_cluster_key": "etl_cluster",
      "new_cluster": {
        "spark_version": "13.3.x-scala2.12",
        "node_type_id": "m5.2xlarge",
        "num_workers": 3,
        "autoscale": { "min_workers": 2, "max_workers": 5 }
      }
    }
  ]
}
```

---

## 4. Cluster Configuration

| Parameter | Recommended Value | Reason |
|---|---|---|
| **DBR Version** | 13.3 LTS | Long-term support, Delta 2.4, Spark 3.5 |
| **Driver type** | `m5.xlarge` (4 cores / 16 GB) | Orchestration overhead |
| **Worker type** | `m5.2xlarge` (8 cores / 32 GB) | Handle 1.7 GB Sales file efficiently |
| **Worker count** | 3 autoscale → 5 max | Balance cost vs speed |
| **Spot instances** | Yes (workers only) | ~60-70% cost reduction; driver stays on-demand |
| **Single-user mode** | Recommended | Avoids resource contention |

### Auto-termination

Enable **auto-terminate after 30 minutes** of inactivity to avoid idle charges.

---

## 5. Scheduling & Triggering

### Option A — Periodic Schedule (e.g., monthly refresh when new data arrives)

In the Workflow UI:
- Click **Add trigger**
- **Type:** Scheduled
- **Cron:** `0 2 1 * *` (2:00 AM on the 1st of each month)

### Option B — File Arrival Trigger

If new CSVs are loaded to the Volume automatically (e.g., via SFTP drop):
```python
# In 01_bronze_ingestion.py, add Auto Loader for incremental ingestion:
df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "csv")
         .option("cloudFiles.schemaLocation", "/Volumes/.../schema/")
         .load("/Volumes/annie_magic_numbers/bronze/raw_files/")
)
```
This enables event-driven, continuous ingestion without scheduled polling.

### Option C — Manual (current challenge setup)

Run notebooks manually in order:
```
1 → 2 → 3
```

---

## 6. Delta Table Optimization

Run after the initial load or on a weekly maintenance job:

```sql
-- OPTIMIZE + ZORDER speeds up Power BI queries that filter by brand or store
OPTIMIZE annie_magic_numbers.gold.sales_enriched ZORDER BY (brand, store);
OPTIMIZE annie_magic_numbers.gold.product_profitability ZORDER BY (brand);
OPTIMIZE annie_magic_numbers.gold.brand_profitability ZORDER BY (brand);
OPTIMIZE annie_magic_numbers.gold.sales_time_series ZORDER BY (sale_year, sale_month);

-- VACUUM removes old Delta versions (keep 7-day history for time-travel)
VACUUM annie_magic_numbers.gold.sales_enriched RETAIN 168 HOURS;
VACUUM annie_magic_numbers.silver.sales RETAIN 168 HOURS;
```

Enable Delta **auto-optimization** for append-heavy tables:
```sql
ALTER TABLE annie_magic_numbers.silver.sales
SET TBLPROPERTIES (
  'delta.autoOptimize.optimizeWrite' = 'true',
  'delta.autoOptimize.autoCompact'   = 'true'
);
```

---

## 7. Monitoring & Alerting

### Job-level Monitoring

In the Workflow UI → **Job runs** tab:
- View run duration, task-level status, and Spark UI
- Click a failed task to see error logs and stack traces

### Email Alerts

In the Workflow → **Edit** → **Job notifications**:
```
On failure  → data-team@company.com
On success  → annie@distributors.com
```

### Custom Data Quality Alert (add to `03_gold_metrics.py`)

```python
from databricks.sdk.runtime import dbutils

# Alert if Gold tables have suspicious row counts
gold_sales_count = spark.table("annie_magic_numbers.gold.sales_enriched").count()
if gold_sales_count < 1_000_000:   # expected: several million rows
    dbutils.notebook.exit(f"ALERT: gold.sales_enriched has only {gold_sales_count} rows — check source data!")
```

### Databricks SQL Alerts

Create a SQL Alert for data freshness:
```sql
-- In Databricks SQL → Alerts → New Alert
SELECT MAX(sales_date) AS latest_sale
FROM annie_magic_numbers.gold.sales_enriched;
-- Alert if latest_sale < CURRENT_DATE - 40 (data is stale)
```

---

## 8. CI/CD with Databricks Asset Bundles

[DABs](https://docs.databricks.com/dev-tools/bundles/index.html) allow you to version and deploy the pipeline from a Git repo.

### File: `databricks.yml`

```yaml
bundle:
  name: annie-magic-numbers

targets:
  dev:
    mode: development
    workspace:
      host: https://<your-workspace>.azuredatabricks.net
  prod:
    mode: production
    workspace:
      host: https://<your-workspace>.azuredatabricks.net

resources:
  jobs:
    annie_etl_pipeline:
      name: "Annie Magic Numbers — ETL Pipeline"
      tasks:
        - task_key: bronze_ingestion
          notebook_task:
            notebook_path: ./databricks/01_bronze_ingestion
          job_cluster_key: etl_cluster
        - task_key: silver_cleaning
          depends_on: [{ task_key: bronze_ingestion }]
          notebook_task:
            notebook_path: ./databricks/02_silver_cleaning
          job_cluster_key: etl_cluster
        - task_key: gold_metrics
          depends_on: [{ task_key: silver_cleaning }]
          notebook_task:
            notebook_path: ./databricks/03_gold_metrics
          job_cluster_key: etl_cluster
      job_clusters:
        - job_cluster_key: etl_cluster
          new_cluster:
            spark_version: "13.3.x-scala2.12"
            node_type_id: m5.2xlarge
            autoscale: { min_workers: 2, max_workers: 5 }
```

### Deploy Commands

```bash
# Install
pip install databricks-cli
databricks bundle validate   # validate configuration
databricks bundle deploy     # deploy to dev
databricks bundle run annie_etl_pipeline  # trigger a run
```

### GitHub Actions Integration

```yaml
# .github/workflows/deploy.yml
name: Deploy Databricks Pipeline
on:
  push:
    branches: [main]
jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - name: Deploy to Databricks
        run: |
          pip install databricks-cli
          databricks bundle deploy --target prod
        env:
          DATABRICKS_HOST: ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
```

---

## 9. Access Control & Row-Level Security

### Power BI Service Principal

```sql
-- Create dedicated read-only role for Power BI
CREATE ROLE IF NOT EXISTS powerbi_reader;
GRANT SELECT ON SCHEMA annie_magic_numbers.gold TO ROLE powerbi_reader;

-- Assign to service principal
GRANT ROLE powerbi_reader TO SERVICE PRINCIPAL `powerbi-sp`;
```

### Row-Level Security (if needed by store/region)

```sql
-- Example: limit store managers to see only their store's data
CREATE ROW FILTER annie_magic_numbers.gold.sales_by_store
AS (store) -> store = current_user_store();
```

---

## 10. Estimated Compute Costs

> Estimates for AWS `m5.2xlarge` worker nodes (DBU pricing varies by cloud provider)

| Run type | Duration | DBUs | Est. cost/run |
|---|---|---|---|
| Initial full load (3 notebooks) | ~45–60 min | ~12 DBUs | ~$5–8 |
| Monthly incremental refresh | ~20–30 min | ~5 DBUs | ~$2–4 |
| Optimization job (weekly) | ~10 min | ~2 DBUs | ~$1 |

Enable **Spot/Preemptible workers** to reduce costs by ~65%.
