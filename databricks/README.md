# Annie's Magic Numbers — Databricks Medallion Pipeline

> **Challenge:** Annie, owner of a large US liquor & spirits distributor, wants to understand profits and margins across her product catalog.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────┐
│                        SOURCE DATA (CSV Files)                      │
│  BegInv │ EndInv │ Purchases │ Sales │ InvoicePurchases │ PurchPrices │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Spark CSV Reader
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥉 BRONZE LAYER  (main.bronze.*)                                   │
│  Raw Delta tables — no transformation, all columns preserved        │
│  + _ingestion_timestamp  + _source_file  (lineage metadata)         │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Type casting, dedup, null dropping
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥈 SILVER LAYER  (main.silver.*)                                   │
│  Cleaned, typed, enriched — cost_per_unit resolved via join         │
└───────────────────────────┬─────────────────────────────────────────┘
                            │  Spark SQL aggregations + window fns
                            ▼
┌─────────────────────────────────────────────────────────────────────┐
│  🥇 GOLD LAYER    (main.gold.*)                                     │
│  10 analytical tables → consumed directly by Power BI               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Notebooks — Run Order

| # | Notebook | Layer | Description |
|---|---|---|---|
| 1 | `01_bronze_ingestion.py` | Bronze | Ingest 6 raw CSVs → Delta tables |
| 2 | `02_silver_cleaning.py` | Silver | Clean, type-cast, deduplicate, enrich |
| 3 | `03_gold_metrics.py` | Gold | Business metrics, profit, margin, rankings |

---

## Delta Table Schemas

### 🥉 Bronze Tables

| Table | Source File | Key Columns |
|---|---|---|
| `bronze.beg_inventory` | BegInvFINAL12312016.csv | InventoryId, Store, Brand, Description, Size, onHand, Price |
| `bronze.end_inventory` | EndInvFINAL12312016.csv | same as beg_inventory + endDate |
| `bronze.purchases` | PurchasesFINAL12312016.csv | Brand, Description, VendorNumber, PONumber, PurchasePrice, Quantity, Dollars |
| `bronze.sales` | SalesFINAL12312016.csv | Store, Brand, Description, SalesQuantity, SalesDollars, SalesDate |
| `bronze.invoice_purchases` | InvoicePurchases12312016.csv | VendorNumber, PONumber, InvoiceDate, Quantity, Dollars |
| `bronze.purchase_prices` | 2017PurchasePricesDec.csv | Brand, Description, Price |

### 🥈 Silver Tables

Same as Bronze but with:
- Lowercase snake_case column names
- Correct types (`DoubleType`, `IntegerType`, `DateType`)
- `_ingestion_timestamp` and `_source_file` dropped
- `silver.purchases` enriched with `cost_per_unit` from `silver.purchase_prices`

### 🥇 Gold Tables

| Table | Purpose | Key Metrics |
|---|---|---|
| `gold.sales_enriched` | Row-level fact table | `profit_dollars`, `margin_pct`, `cost_per_unit` |
| `gold.product_profitability` | Product-level aggregation | `total_profit_dollars`, `avg_margin_pct`, `rank_by_profit`, `rank_by_margin` |
| `gold.brand_profitability` | Brand-level aggregation | same as product + `sku_count` |
| `gold.loss_makers` | Negative-profit items | `total_profit_dollars < 0`, `recommendation` |
| `gold.sales_by_store` | Store performance | `total_revenue`, `total_profit_dollars`, `avg_margin_pct` |
| `gold.sales_time_series` | Monthly sales trend | `monthly_revenue`, `monthly_profit`, `avg_margin_pct` |
| `gold.inventory_delta` | Beg vs End inventory | `inventory_change`, `stock_status` |
| `gold.vendor_performance` | Vendor spend & lead time | `total_purchase_spend`, `avg_lead_time_days` |
| `gold.size_analysis` | Pack/bottle size breakdown | `total_profit_dollars`, `avg_selling_price` |
| `gold.classification_performance` | Spirit category analysis | `total_revenue`, `overall_margin_pct` |

---

## Business Logic

```
Profit ($) = SalesDollars − (cost_per_unit × SalesQuantity)
Margin (%) = Profit / SalesDollars × 100

cost_per_unit priority:
  1. Median PurchasePrice from PurchasesFINAL (most accurate — actual PO data)
  2. Fallback: SalesPrice × 0.60 heuristic (when no purchase record found)
```

---

## Setup Instructions

### 1. Upload Source Files to DBFS

```bash
# Using Databricks CLI (install: pip install databricks-cli)
databricks fs mkdirs dbfs:/FileStore/annie_magic_numbers
databricks fs cp files/ dbfs:/FileStore/annie_magic_numbers/ --recursive
```

Or via Databricks UI: **Data → Upload → DBFS → FileStore/annie_magic_numbers/**

### 2. Create a Cluster

- **Runtime:** Databricks Runtime 13.3 LTS or later
- **Node type:** `m5.2xlarge` (8 cores / 32 GB RAM) minimum for the Sales file
- **Workers:** 2–4 auto-scaling workers recommended

### 3. Import Notebooks

- In Databricks workspace: **Workspace → Import → upload .py files**
- Alternatively, use [Databricks Repos](https://docs.databricks.com/repos/index.html) connected to your GitHub repo

### 4. Update Config Variables

In each notebook's `Configuration` cell, update:
```python
DBFS_ROOT = "/FileStore/annie_magic_numbers"  # where you uploaded the files
CATALOG   = "main"                            # your Unity Catalog name
```

### 5. Run Notebooks in Order

```
01_bronze_ingestion.py  →  02_silver_cleaning.py  →  03_gold_metrics.py
```

### 6. Connect Power BI

- **Home → Partner Connect → Power BI**
- Click **Download .pbids file**
- Open in Power BI Desktop; connect to `gold.*` tables

---

## Libraries

All libraries are pre-installed on Databricks Runtime 13.3+ — no pip installs needed:
- `pyspark` (Spark SQL, DataFrame API, Window functions)
- `delta` (Delta Lake format)
- `datetime` (standard Python)

---

## Expected Runtimes (on 4-worker cluster)

| Notebook | Estimated Time |
|---|---|
| Bronze ingestion | 10–20 min (dominated by 1.7 GB Sales file) |
| Silver cleaning | 15–25 min (Sales dedup is the bottleneck) |
| Gold metrics | 5–15 min (aggregations are fast on Silver partitioned tables) |

---

## Project Structure

```
Baselabs/
├── files/                          # Raw source CSV files (uploaded to DBFS)
├── databricks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_cleaning.py
│   ├── 03_gold_metrics.py
│   └── README.md                   ← you are here
├── productionization_guide.md      # How to productionize in Databricks
└── powerbi_dashboard_guide.md      # Power BI 2-page dashboard spec
```
