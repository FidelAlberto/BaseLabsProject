# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer — Raw Ingestion
# MAGIC
# MAGIC **Medallion Architecture: Bronze → Silver → Gold**
# MAGIC
# MAGIC This notebook ingests the 6 raw CSV files provided for the Annie's Magic Numbers
# MAGIC challenge into Delta tables with zero business transformation.
# MAGIC
# MAGIC | Source File | Bronze Table |
# MAGIC |---|---|
# MAGIC | BegInvFINAL12312016.csv | bronze.beg_inventory |
# MAGIC | EndInvFINAL12312016.csv | bronze.end_inventory |
# MAGIC | PurchasesFINAL12312016.csv | bronze.purchases |
# MAGIC | SalesFINAL12312016.csv | bronze.sales |
# MAGIC | InvoicePurchases12312016.csv | bronze.invoice_purchases |
# MAGIC | 2017PurchasePricesDec.csv | bronze.purchase_prices |
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Azure Data Lake Configuration

# COMMAND ----------

# ============================================================
# CELL 0 — ADLS Gen2 Authentication (Storage Account Key)
# ============================================================

spark.conf.set(
    "fs.azure.account.key.anniedatalake123.dfs.core.windows.net",
    "<YOUR_STORAGE_ACCOUNT_KEY>"
)
# ============================================================
# Azure Data Lake Gen2 base paths
# ============================================================
# Se definen los paths de forma centralizada para:
# - Evitar hardcoding
# - Facilitar reutilización en Silver/Gold
# - Permitir cambios de storage sin tocar lógica

container = "annie-data"
storage_account = "anniedatalake123"

base_path = f"abfss://{container}@{storage_account}.dfs.core.windows.net/"
raw_path = base_path + "raw/"
bronze_path = base_path + "bronze/"

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validate RAW Layer Files

# COMMAND ----------

# ============================================================
# CELL 1 — Validate presence of RAW CSV files
# ============================================================
# Ensures that ingestion is not executed on empty or incorrect paths.
# This is a critical early validation step in production pipelines.

dbutils.fs.ls(raw_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generic CSV Reader Function

# COMMAND ----------

# ============================================================
# CELL 2 — Generic CSV Reader Function
# ============================================================
# Reads CSV files from the RAW layer using Spark.
#
# Design decisions:
# - header=True: source files contain headers
# - inferSchema=True: acceptable for Bronze ingestion
#   (explicit schemas will be enforced in Silver)

def read_csv(filename):
    return (
        spark.read
             .option("header", True)
             .option("inferSchema", True)
             .csv(raw_path + filename)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Load RAW CSV Files into DataFrames

# COMMAND ----------

# ============================================================
# CELL 3 — Load RAW CSV Files into DataFrames
# ============================================================
# Each CSV file is loaded into its own DataFrame.
# This preserves lineage and simplifies debugging.

sales_df = read_csv("SalesFINAL12312016.csv")
purchases_df = read_csv("PurchasesFINAL12312016.csv")
prices_df = read_csv("2017PurchasePricesDec.csv")
begin_inventory_df = read_csv("BegInvFINAL12312016.csv")
end_inventory_df = read_csv("EndInvFINAL12312016.csv")
invoices_df = read_csv("InvoicePurchases12312016.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Data Inspection & Schema Validation

# COMMAND ----------

# ============================================================
# CELL 4 — Data Inspection & Schema Validation
# ============================================================
# Visual inspection ensures:
# - Columns are correctly parsed
# - Data types are reasonable
# - No malformed rows or parsing issues

display(sales_df)
display(purchases_df)
display(prices_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Bronze Delta Writer Function
# MAGIC

# COMMAND ----------

# ============================================================
# CELL 6 — Bronze Delta Writer Function
# ============================================================
# Writes DataFrames to the Bronze layer in Delta format.
#
# Design decisions:
# - Delta Lake provides ACID guarantees and schema evolution
# - overwrite mode ensures idempotent pipeline execution
# - One folder per dataset

def write_bronze(df, table_name):
    (
        df.write
          .format("delta")
          .mode("overwrite")
          .save(bronze_path + table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Persist DataFrames to Bronze Layer

# COMMAND ----------

# ============================================================
# CELL 7 — Persist DataFrames to the Bronze Layer
# ============================================================
# Bronze rules applied:
# - No filtering
# - No aggregations
# - No business logic
# - Raw data preserved as-is

write_bronze(sales_df, "sales")
write_bronze(purchases_df, "purchases")
write_bronze(prices_df, "prices")
write_bronze(begin_inventory_df, "begin_inventory")
write_bronze(end_inventory_df, "end_inventory")
write_bronze(invoices_df, "invoices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Validate Bronze Folder Structure

# COMMAND ----------

# ============================================================
# CELL 8 — Validate Bronze Folder Structure
# ============================================================
# Confirms:
# - Delta folders were created
# - _delta_log exists for each dataset
# - Parquet files are present

dbutils.fs.ls(bronze_path)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Validate Delta Read from Bronze

# COMMAND ----------

# ============================================================
# CELL 9 — Validate Delta Read from Bronze
# ============================================================
# Confirms:
# - Delta tables are readable
# - Data is ready for Silver transformations

sales_bronze_df = (
    spark.read
         .format("delta")
         .load(bronze_path + "sales")
)

display(sales_bronze_df)