# Databricks notebook source
# MAGIC %md
# MAGIC # 🥈 Silver Layer — Cleaning & Enrichment
# MAGIC
# MAGIC **Medallion Architecture: Bronze → Silver → Gold**
# MAGIC
# MAGIC This notebook reads each Bronze Delta table, applies the following transformations,
# MAGIC and writes clean Silver Delta tables:
# MAGIC
# MAGIC | Step | What happens |
# MAGIC |---|---|
# MAGIC | Column normalization | Lowercase + underscore column names |
# MAGIC | Type casting | Prices → DoubleType, Quantities → IntegerType, Dates → DateType |
# MAGIC | Null handling | Drop rows without primary business keys or critical financial fields |
# MAGIC | Deduplication | Remove exact duplicates on business key combinations |
# MAGIC | Cost enrichment | Join `purchases` with `purchase_prices` to produce `cost_per_unit` |
# MAGIC
# MAGIC ---
# MAGIC **Author:** Data Engineering Team  
# MAGIC **Last Updated:** 2026-02-24  
# MAGIC **Run Order:** 2 of 3 (requires `01_bronze_ingestion.ipynb` to have run first)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, DateType
)

CATALOG      = "main"             # ← UPDATE if using a different Unity Catalog
BRONZE_DB    = "bronze"
SILVER_DB    = "silver"

BRONZE_SCHEMA = f"{CATALOG}.{BRONZE_DB}"
SILVER_SCHEMA = f"{CATALOG}.{SILVER_DB}"

print(f"Reading from : {BRONZE_SCHEMA}")
print(f"Writing to   : {SILVER_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Silver Database / Schema

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_SCHEMA}")
print(f"✅  Schema '{SILVER_SCHEMA}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Helper Functions

# COMMAND ----------

def normalize_columns(df):
    """
    Normalize column names to lowercase with underscores.
    e.g.  'SalesQuantity' → 'sales_quantity'
    """
    import re
    def to_snake(name):
        s1 = re.sub(r'(.)([A-Z][a-z]+)', r'\1_\2', name)
        return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', s1).lower().strip()

    new_cols = {c: to_snake(c) for c in df.columns if not c.startswith("_")}
    for old, new in new_cols.items():
        if old != new:
            df = df.withColumnRenamed(old, new)
    return df


def write_silver(df, table_name: str, partition_by: str = None) -> None:
    """Write a cleaned DataFrame to the Silver layer as a Delta table."""
    full_name = f"{SILVER_SCHEMA}.{table_name}"
    writer = (
        df.write
          .format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
    )
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.saveAsTable(full_name)
    count = spark.table(full_name).count()
    print(f"   ✅  {full_name}  →  {count:,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Silver — Beginning Inventory

# COMMAND ----------

print("Processing silver.beg_inventory ...")

beg_inv_raw = spark.table(f"{BRONZE_SCHEMA}.beg_inventory")
beg_inv = normalize_columns(beg_inv_raw)

beg_inv = (
    beg_inv
    # Drop metadata columns not needed in Silver
    .drop("_ingestion_timestamp", "_source_file")

    # Cast numeric fields
    .withColumn("on_hand",    F.col("on_hand").cast(IntegerType()))
    .withColumn("price",      F.col("price").cast(DoubleType()))
    .withColumn("total_cost", F.col("total_cost").cast(DoubleType()))

    # Parse date
    .withColumn("start_date", F.to_date(F.col("start_date"), "yyyy-MM-dd"))

    # Drop rows missing critical identifiers
    .filter(F.col("inventory_id").isNotNull())
    .filter(F.col("brand").isNotNull())
    .filter(F.col("price") > 0)

    # Deduplicate on primary key
    .dropDuplicates(["inventory_id"])

    # Derived field: cost basis = on-hand qty × purchase price
    .withColumn("inventory_value", F.round(F.col("on_hand") * F.col("price"), 2))
)

write_silver(beg_inv, "beg_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Silver — Ending Inventory

# COMMAND ----------

print("Processing silver.end_inventory ...")

end_inv_raw = spark.table(f"{BRONZE_SCHEMA}.end_inventory")
end_inv = normalize_columns(end_inv_raw)

end_inv = (
    end_inv
    .drop("_ingestion_timestamp", "_source_file")
    .withColumn("on_hand",    F.col("on_hand").cast(IntegerType()))
    .withColumn("price",      F.col("price").cast(DoubleType()))
    .withColumn("total_cost", F.col("total_cost").cast(DoubleType()))
    .withColumn("end_date",   F.to_date(F.col("end_date"), "yyyy-MM-dd"))
    .filter(F.col("inventory_id").isNotNull())
    .filter(F.col("brand").isNotNull())
    .filter(F.col("price") > 0)
    .dropDuplicates(["inventory_id"])
    .withColumn("inventory_value", F.round(F.col("on_hand") * F.col("price"), 2))
)

write_silver(end_inv, "end_inventory")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Silver — Reference Purchase Prices (2017 Dec)
# MAGIC
# MAGIC This table provides the **cost basis** used to compute profit per unit sold.
# MAGIC It is the most reliable cost reference because it reflects actual wholesale prices.

# COMMAND ----------

print("Processing silver.purchase_prices ...")

pp_raw = spark.table(f"{BRONZE_SCHEMA}.purchase_prices")
pp = normalize_columns(pp_raw)

pp = (
    pp
    .drop("_ingestion_timestamp", "_source_file")
    .withColumn("price", F.col("price").cast(DoubleType()))
    .filter(F.col("brand").isNotNull())
    .filter(F.col("description").isNotNull())
    .filter(F.col("price").isNotNull())
    .filter(F.col("price") > 0)
    # Brand in this file is a numeric ID in raw form; we keep it as-is for joining
    .withColumn("brand", F.col("brand").cast(IntegerType()))
    .dropDuplicates(["brand", "description"])
)

write_silver(pp, "purchase_prices")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Silver — Invoice Purchases

# COMMAND ----------

print("Processing silver.invoice_purchases ...")

inv_raw = spark.table(f"{BRONZE_SCHEMA}.invoice_purchases")
inv = normalize_columns(inv_raw)

inv = (
    inv
    .drop("_ingestion_timestamp", "_source_file")
    .withColumn("vendor_number",  F.col("vendor_number").cast(IntegerType()))
    .withColumn("quantity",       F.col("quantity").cast(IntegerType()))
    .withColumn("dollars",        F.col("dollars").cast(DoubleType()))
    .withColumn("freight",        F.col("freight").cast(DoubleType()))
    .withColumn("invoice_date",   F.to_date(F.col("invoice_date"), "MM/dd/yyyy"))
    .withColumn("pay_date",       F.to_date(F.col("pay_date"), "MM/dd/yyyy"))
    .filter(F.col("po_number").isNotNull())
    .dropDuplicates(["vendor_number", "po_number", "invoice_date"])
)

write_silver(inv, "invoice_purchases")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Silver — Purchases (with cost_per_unit enrichment)
# MAGIC
# MAGIC The `purchases` table records every purchase order line in 2016.
# MAGIC We enrich it by joining with `silver.purchase_prices` (the 2017 Dec price list)
# MAGIC to ensure a consistent `cost_per_unit` that reflects the actual wholesale cost.
# MAGIC
# MAGIC **Fallback**: If no match found in purchase_prices, we use the raw `purchase_price`
# MAGIC column already present in the purchases table.

# COMMAND ----------

print("Processing silver.purchases ...")

purch_raw = spark.table(f"{BRONZE_SCHEMA}.purchases")
purch = normalize_columns(purch_raw)

purch = (
    purch
    .drop("_ingestion_timestamp", "_source_file")
    # Cast types
    .withColumn("vendor_number",   F.col("vendor_number").cast(IntegerType()))
    .withColumn("quantity",        F.col("quantity").cast(IntegerType()))
    .withColumn("dollars",         F.col("dollars").cast(DoubleType()))
    .withColumn("purchase_price",  F.col("purchase_price").cast(DoubleType()))
    .withColumn("brand",           F.col("brand").cast(IntegerType()))
    # Parse dates
    .withColumn("po_date",         F.to_date(F.col("po_date"), "MM/dd/yyyy"))
    .withColumn("receiving_date",  F.to_date(F.col("receiving_date"), "MM/dd/yyyy"))
    .withColumn("invoice_date",    F.to_date(F.col("invoice_date"), "MM/dd/yyyy"))
    # Drop rows without critical fields
    .filter(F.col("brand").isNotNull())
    .filter(F.col("description").isNotNull())
    .filter(F.col("quantity") > 0)
    .dropDuplicates(["vendor_number", "po_number", "brand", "description", "receiving_date"])
)

# ------------------------------------------------------------------
# Enrich with reference purchase prices
# ------------------------------------------------------------------
pp_silver = spark.table(f"{SILVER_SCHEMA}.purchase_prices") \
                 .select(
                     F.col("brand").alias("ref_brand"),
                     F.col("description").alias("ref_description"),
                     F.col("price").alias("ref_price")
                 )

purch_enriched = (
    purch
    .join(
        pp_silver,
        on=[purch["brand"] == pp_silver["ref_brand"],
            purch["description"] == pp_silver["ref_description"]],
        how="left"
    )
    # cost_per_unit: prefer the reference price; fall back to PO purchase price
    .withColumn(
        "cost_per_unit",
        F.round(F.coalesce(F.col("ref_price"), F.col("purchase_price")), 4)
    )
    .drop("ref_brand", "ref_description", "ref_price")
    # Derived: total line cost
    .withColumn("total_cost", F.round(F.col("cost_per_unit") * F.col("quantity"), 2))
)

write_silver(purch_enriched, "purchases", partition_by="brand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Silver — Sales (the main revenue table)
# MAGIC
# MAGIC The sales table is the largest file (~1.7 GB / millions of rows).
# MAGIC We partition it by `brand` to optimize Gold layer aggregations.

# COMMAND ----------

print("Processing silver.sales  (large file — may take several minutes) ...")

sales_raw = spark.table(f"{BRONZE_SCHEMA}.sales")
sales = normalize_columns(sales_raw)

sales = (
    sales
    .drop("_ingestion_timestamp", "_source_file")
    # Cast numeric fields
    .withColumn("brand",           F.col("brand").cast(IntegerType()))
    .withColumn("sales_quantity",  F.col("sales_quantity").cast(IntegerType()))
    .withColumn("sales_dollars",   F.col("sales_dollars").cast(DoubleType()))
    .withColumn("sales_price",     F.col("sales_price").cast(DoubleType()))
    .withColumn("excise_tax",      F.col("excise_tax").cast(DoubleType()))
    .withColumn("volume",          F.col("volume").cast(DoubleType()))
    # Parse date
    .withColumn("sales_date",      F.to_date(F.col("sales_date"), "MM/dd/yyyy"))
    # Derived time columns for easy time-series analysis in Gold
    .withColumn("sale_year",       F.year("sales_date"))
    .withColumn("sale_month",      F.month("sales_date"))
    .withColumn("sale_month_name", F.date_format("sales_date", "MMMM"))
    .withColumn("sale_week",       F.weekofyear("sales_date"))
    # Quality filters
    .filter(F.col("brand").isNotNull())
    .filter(F.col("sales_dollars") > 0)
    .filter(F.col("sales_quantity") > 0)
    .filter(F.col("sales_date").isNotNull())
    .dropDuplicates(["store", "brand", "description", "sales_date", "sales_quantity", "sales_dollars"])
)

write_silver(sales, "sales", partition_by="brand")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Validation — Silver Layer Summary

# COMMAND ----------

silver_tables = [
    "beg_inventory",
    "end_inventory",
    "purchases",
    "sales",
    "invoice_purchases",
    "purchase_prices",
]

print("\n📊  Silver Layer Validation Report")
print("=" * 65)

for tbl in silver_tables:
    full_name = f"{SILVER_SCHEMA}.{tbl}"
    df = spark.table(full_name)
    count    = df.count()
    nullable = sum(df.filter(F.col(c).isNull()).count() > 0 for c in df.columns[:5])
    print(f"  {tbl:<25} | {count:>12,} rows | null cols (first 5): {nullable}")

print("=" * 65)

# Spot-check: cost_per_unit coverage in purchases
purch_silver = spark.table(f"{SILVER_SCHEMA}.purchases")
missing_cost = purch_silver.filter(F.col("cost_per_unit").isNull()).count()
total_purch  = purch_silver.count()
pct_covered  = 100 * (1 - missing_cost / max(total_purch, 1))
print(f"\n  cost_per_unit coverage  : {pct_covered:.1f}%  ({total_purch - missing_cost:,} / {total_purch:,} rows)")

print("\n✅  Silver layer ready.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Silver Cleaning Complete
# MAGIC
# MAGIC **Next step:** Run `03_gold_metrics.ipynb` to build all analytical Gold tables.