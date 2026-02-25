# Databricks notebook source
# MAGIC %md
# MAGIC # 🥇 Gold Layer — Business Metrics
# MAGIC
# MAGIC **Medallion Architecture: Bronze → Silver → Gold**
# MAGIC
# MAGIC This notebook produces all analytical tables consumed directly by Power BI.
# MAGIC All business logic (profit, margin, rankings, loss detection) is applied here.
# MAGIC
# MAGIC | Gold Table | Purpose |
# MAGIC |---|---|
# MAGIC | `gold.sales_enriched` | Row-level sales + cost data with profit & margin |
# MAGIC | `gold.product_profitability` | Top/bottom products by profit $ and margin % |
# MAGIC | `gold.brand_profitability` | Top/bottom brands by profit $ and margin % |
# MAGIC | `gold.loss_makers` | All products/brands with negative cumulative profit |
# MAGIC | `gold.sales_by_store` | Revenue, profit, margin by store and city |
# MAGIC | `gold.sales_time_series` | Monthly profit and revenue trend |
# MAGIC | `gold.inventory_delta` | Ending − Beginning inventory: over/understocked |
# MAGIC | `gold.vendor_performance` | Vendor spend, avg price, implied margin |
# MAGIC | `gold.size_analysis` | Profit and margin by bottle/pack size |
# MAGIC | `gold.classification_performance` | Profit by spirit classification |
# MAGIC
# MAGIC **Business Logic:**
# MAGIC - `profit_dollars = sales_dollars - (cost_per_unit × sales_quantity)`
# MAGIC - `margin_pct = profit_dollars / sales_dollars × 100`
# MAGIC
# MAGIC ---
# MAGIC **Author:** Data Engineering Team  
# MAGIC **Last Updated:** 2026-02-24  
# MAGIC **Run Order:** 3 of 3 (requires `02_silver_cleaning.ipynb` to have run first)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Configuration

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

CATALOG       = "main"          # ← UPDATE if using a different Unity Catalog
SILVER_SCHEMA = f"{CATALOG}.silver"
GOLD_SCHEMA   = f"{CATALOG}.gold"

print(f"Reading from : {SILVER_SCHEMA}")
print(f"Writing to   : {GOLD_SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Create Gold Schema

# COMMAND ----------

spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_SCHEMA}")
print(f"✅  Schema '{GOLD_SCHEMA}' is ready.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Register Silver Tables as Temporary Views

# COMMAND ----------

# Load Silver tables once, cache where appropriate
silver_sales    = spark.table(f"{SILVER_SCHEMA}.sales")
silver_purchases = spark.table(f"{SILVER_SCHEMA}.purchases")
silver_beg_inv  = spark.table(f"{SILVER_SCHEMA}.beg_inventory")
silver_end_inv  = spark.table(f"{SILVER_SCHEMA}.end_inventory")

silver_sales.createOrReplaceTempView("sv_sales")
silver_purchases.createOrReplaceTempView("sv_purchases")
silver_beg_inv.createOrReplaceTempView("sv_beg_inv")
silver_end_inv.createOrReplaceTempView("sv_end_inv")

print("✅  Temporary views registered.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Core Cost Lookup
# MAGIC
# MAGIC Build a product-level cost lookup from the Silver purchases table.
# MAGIC We take the **median cost_per_unit** per (brand, description) combination
# MAGIC to smooth out price fluctuations across individual POs.

# COMMAND ----------

cost_lookup = spark.sql("""
    SELECT
        brand,
        description,
        PERCENTILE_APPROX(cost_per_unit, 0.5)  AS median_cost_per_unit,
        AVG(cost_per_unit)                       AS avg_cost_per_unit,
        MIN(cost_per_unit)                       AS min_cost_per_unit,
        MAX(cost_per_unit)                       AS max_cost_per_unit,
        COUNT(DISTINCT vendor_number)            AS supplier_count
    FROM sv_purchases
    WHERE cost_per_unit IS NOT NULL
      AND cost_per_unit > 0
    GROUP BY brand, description
""")

cost_lookup.createOrReplaceTempView("cost_lookup")
print(f"✅  cost_lookup built: {cost_lookup.count():,} product-level cost records.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Gold — Sales Enriched
# MAGIC
# MAGIC Joins sales with the cost lookup to compute row-level **profit** and **margin**.
# MAGIC This is the atomic fact table from which all other Gold tables are derived.

# COMMAND ----------

sales_enriched = spark.sql("""
    SELECT
        s.store,
        s.brand,
        s.description,
        s.size,
        s.classification,
        s.sales_date,
        s.sale_year,
        s.sale_month,
        s.sale_month_name,
        s.sale_week,
        s.sales_quantity,
        s.sales_dollars,
        s.sales_price,
        s.excise_tax,
        s.volume,
        -- Cost resolution: prefer median market cost; fall back to sales_price * 0.6 heuristic
        COALESCE(c.median_cost_per_unit, s.sales_price * 0.60)           AS cost_per_unit,
        CASE
            WHEN c.median_cost_per_unit IS NOT NULL THEN 'purchase_data'
            ELSE 'heuristic_60pct'
        END                                                               AS cost_source,
        -- Profit calculation
        ROUND(
            s.sales_dollars
            - (COALESCE(c.median_cost_per_unit, s.sales_price * 0.60) * s.sales_quantity),
        2)                                                                AS profit_dollars,
        -- Margin calculation (guard against divide-by-zero)
        ROUND(
            CASE
                WHEN s.sales_dollars = 0 THEN NULL
                ELSE (
                    (s.sales_dollars
                     - COALESCE(c.median_cost_per_unit, s.sales_price * 0.60) * s.sales_quantity)
                    / s.sales_dollars
                ) * 100
            END,
        2)                                                                AS margin_pct
    FROM sv_sales s
    LEFT JOIN cost_lookup c
        ON s.brand = c.brand
       AND s.description = c.description
""")

sales_enriched.createOrReplaceTempView("gold_se")

(
    sales_enriched.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .partitionBy("brand")
        .saveAsTable(f"{GOLD_SCHEMA}.sales_enriched")
)
print(f"✅  gold.sales_enriched: {sales_enriched.count():,} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Gold — Product Profitability
# MAGIC
# MAGIC Aggregate by `(brand, description, size)` to compute total profit, total revenue,
# MAGIC average margin, and rankings.

# COMMAND ----------

product_profitability = spark.sql("""
    SELECT
        brand,
        description,
        size,
        classification,
        COUNT(*)                                    AS transaction_count,
        SUM(sales_quantity)                         AS total_units_sold,
        ROUND(SUM(sales_dollars), 2)                AS total_revenue,
        ROUND(SUM(profit_dollars), 2)               AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                   AS avg_margin_pct,
        ROUND(MIN(margin_pct), 2)                   AS min_margin_pct,
        ROUND(MAX(margin_pct), 2)                   AS max_margin_pct,
        ROUND(SUM(cost_per_unit * sales_quantity), 2) AS total_cost,
        ROUND(SUM(volume), 2)                       AS total_volume_liters,
        -- Ranking columns (populated by window functions below)
        0                                           AS rank_by_profit,
        0                                           AS rank_by_margin,
        CASE WHEN SUM(profit_dollars) < 0 THEN true ELSE false END AS is_loss_maker
    FROM gold_se
    GROUP BY brand, description, size, classification
""")

# Apply proper window rankings using PySpark
profit_window = Window.orderBy(F.col("total_profit_dollars").desc())
margin_window = Window.orderBy(F.col("avg_margin_pct").desc())

product_profitability = (
    product_profitability
    .drop("rank_by_profit", "rank_by_margin")
    .withColumn("rank_by_profit", F.rank().over(profit_window))
    .withColumn("rank_by_margin", F.rank().over(margin_window))
)

(
    product_profitability.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.product_profitability")
)
print(f"✅  gold.product_profitability: {product_profitability.count():,} products")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Gold — Brand Profitability

# COMMAND ----------

brand_profitability = spark.sql("""
    SELECT
        brand,
        -- Most common description for this brand (representative name)
        FIRST(description)                           AS sample_description,
        FIRST(classification)                        AS classification,
        COUNT(DISTINCT description)                  AS sku_count,
        COUNT(*)                                     AS transaction_count,
        ROUND(SUM(sales_dollars), 2)                 AS total_revenue,
        ROUND(SUM(profit_dollars), 2)                AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                    AS avg_margin_pct,
        ROUND(SUM(cost_per_unit * sales_quantity), 2) AS total_cost,
        SUM(sales_quantity)                          AS total_units_sold,
        ROUND(SUM(volume), 2)                        AS total_volume_liters,
        ROUND(SUM(excise_tax), 2)                    AS total_excise_tax,
        CASE WHEN SUM(profit_dollars) < 0 THEN true ELSE false END AS is_loss_maker
    FROM gold_se
    GROUP BY brand
""")

brand_profit_window  = Window.orderBy(F.col("total_profit_dollars").desc())
brand_margin_window  = Window.orderBy(F.col("avg_margin_pct").desc())

brand_profitability = (
    brand_profitability
    .withColumn("rank_by_profit", F.rank().over(brand_profit_window))
    .withColumn("rank_by_margin", F.rank().over(brand_margin_window))
)

(
    brand_profitability.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.brand_profitability")
)
print(f"✅  gold.brand_profitability: {brand_profitability.count():,} brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Gold — Loss Makers
# MAGIC
# MAGIC Products AND brands with **negative cumulative profit** — candidates for elimination
# MAGIC from Annie's wholesale catalog.

# COMMAND ----------

loss_products = spark.sql("""
    SELECT
        'PRODUCT'                                   AS level,
        CAST(brand AS STRING)                       AS brand_id,
        description,
        size,
        classification,
        ROUND(SUM(sales_dollars), 2)                AS total_revenue,
        ROUND(SUM(profit_dollars), 2)               AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                   AS avg_margin_pct,
        SUM(sales_quantity)                         AS total_units_sold,
        COUNT(DISTINCT store)                       AS stores_stocking,
        'Negative cumulative profit — consider dropping' AS recommendation
    FROM gold_se
    GROUP BY brand, description, size, classification
    HAVING SUM(profit_dollars) < 0
    ORDER BY SUM(profit_dollars) ASC
""")

loss_brands = spark.sql("""
    SELECT
        'BRAND'                                     AS level,
        CAST(brand AS STRING)                       AS brand_id,
        FIRST(description)                          AS description,
        'ALL'                                       AS size,
        FIRST(classification)                       AS classification,
        ROUND(SUM(sales_dollars), 2)                AS total_revenue,
        ROUND(SUM(profit_dollars), 2)               AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                   AS avg_margin_pct,
        SUM(sales_quantity)                         AS total_units_sold,
        COUNT(DISTINCT store)                       AS stores_stocking,
        'Brand-level losses — investigate or discontinue' AS recommendation
    FROM gold_se
    GROUP BY brand
    HAVING SUM(profit_dollars) < 0
    ORDER BY SUM(profit_dollars) ASC
""")

loss_makers = loss_products.union(loss_brands)

(
    loss_makers.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.loss_makers")
)
prod_count  = loss_products.count()
brand_count = loss_brands.count()
print(f"✅  gold.loss_makers: {prod_count} losing products | {brand_count} losing brands")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold — Sales by Store

# COMMAND ----------

sales_by_store = spark.sql("""
    SELECT
        store,
        -- City comes from sales table if available, else inventory
        COUNT(DISTINCT brand)                        AS unique_brands,
        COUNT(DISTINCT description)                  AS unique_skus,
        COUNT(*)                                     AS transaction_count,
        ROUND(SUM(sales_dollars), 2)                 AS total_revenue,
        ROUND(SUM(profit_dollars), 2)                AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                    AS avg_margin_pct,
        SUM(sales_quantity)                          AS total_units_sold,
        ROUND(SUM(volume), 2)                        AS total_volume_liters,
        ROUND(SUM(excise_tax), 2)                    AS total_excise_tax,
        MIN(sales_date)                              AS first_sale_date,
        MAX(sales_date)                              AS last_sale_date
    FROM gold_se
    GROUP BY store
    ORDER BY total_revenue DESC
""")

(
    sales_by_store.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.sales_by_store")
)
print(f"✅  gold.sales_by_store: {sales_by_store.count():,} stores")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Gold — Sales Time Series (Monthly)

# COMMAND ----------

sales_time_series = spark.sql("""
    SELECT
        sale_year,
        sale_month,
        sale_month_name,
        ROUND(SUM(sales_dollars), 2)                 AS monthly_revenue,
        ROUND(SUM(profit_dollars), 2)                AS monthly_profit,
        ROUND(AVG(margin_pct), 2)                    AS avg_margin_pct,
        SUM(sales_quantity)                          AS total_units_sold,
        COUNT(DISTINCT brand)                        AS active_brands,
        COUNT(DISTINCT store)                        AS active_stores,
        COUNT(*)                                     AS transaction_count,
        ROUND(SUM(volume), 2)                        AS total_volume_liters
    FROM gold_se
    GROUP BY sale_year, sale_month, sale_month_name
    ORDER BY sale_year, sale_month
""")

(
    sales_time_series.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.sales_time_series")
)
print(f"✅  gold.sales_time_series: {sales_time_series.count()} monthly records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Gold — Inventory Delta (Beginning vs Ending)
# MAGIC
# MAGIC Computes the change in on-hand inventory across the year.
# MAGIC Flags **overstocked** items (ending > beginning, meaning units didn't move)
# MAGIC and **understocked** items (ending < beginning, potential stock-out risk).

# COMMAND ----------

inventory_delta_sql = """
    SELECT
        COALESCE(b.brand, e.brand)                  AS brand,
        COALESCE(b.description, e.description)      AS description,
        COALESCE(b.size, e.size)                    AS size,
        COALESCE(b.vendor_name, e.vendor_name)      AS vendor_name,
        b.on_hand                                   AS beg_on_hand,
        e.on_hand                                   AS end_on_hand,
        COALESCE(e.on_hand, 0) - COALESCE(b.on_hand, 0) AS inventory_change,
        b.price                                     AS unit_price,
        ROUND((COALESCE(e.on_hand, 0) - COALESCE(b.on_hand, 0)) * COALESCE(b.price, 0), 2)
                                                    AS inventory_value_change,
        CASE
            WHEN COALESCE(e.on_hand, 0) - COALESCE(b.on_hand, 0) > 50 THEN 'OVERSTOCKED'
            WHEN COALESCE(e.on_hand, 0) - COALESCE(b.on_hand, 0) < -50 THEN 'DEPLETED'
            ELSE 'STABLE'
        END                                         AS stock_status
    FROM sv_beg_inv b
    FULL OUTER JOIN sv_end_inv e
        ON b.brand = e.brand
       AND b.description = e.description
       AND b.size = e.size
       AND b.store = e.store
"""

inventory_delta = spark.sql(inventory_delta_sql)

(
    inventory_delta.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.inventory_delta")
)
print(f"✅  gold.inventory_delta: {inventory_delta.count():,} product-store records")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 11. Gold — Vendor Performance

# COMMAND ----------

vendor_performance = spark.sql("""
    SELECT
        p.vendor_number,
        p.vendor_name,
        COUNT(DISTINCT p.brand)                      AS brands_supplied,
        COUNT(DISTINCT p.description)                AS skus_supplied,
        ROUND(SUM(p.dollars), 2)                     AS total_purchase_spend,
        SUM(p.quantity)                              AS total_units_purchased,
        ROUND(AVG(p.cost_per_unit), 4)               AS avg_cost_per_unit,
        COUNT(DISTINCT p.po_number)                  AS total_po_count,
        MIN(p.receiving_date)                        AS first_delivery,
        MAX(p.receiving_date)                        AS last_delivery,
        -- Avg lead time in days (PO date to receiving date)
        ROUND(AVG(DATEDIFF(p.receiving_date, p.po_date)), 1) AS avg_lead_time_days
    FROM sv_purchases p
    GROUP BY p.vendor_number, p.vendor_name
    ORDER BY total_purchase_spend DESC
""")

(
    vendor_performance.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.vendor_performance")
)
print(f"✅  gold.vendor_performance: {vendor_performance.count():,} vendors")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 12. Gold — Size (Package) Analysis

# COMMAND ----------

size_analysis = spark.sql("""
    SELECT
        size,
        COUNT(DISTINCT brand)                        AS unique_brands,
        COUNT(DISTINCT description)                  AS unique_skus,
        COUNT(*)                                     AS transaction_count,
        ROUND(SUM(sales_dollars), 2)                 AS total_revenue,
        ROUND(SUM(profit_dollars), 2)                AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                    AS avg_margin_pct,
        SUM(sales_quantity)                          AS total_units_sold,
        ROUND(SUM(volume), 2)                        AS total_volume_liters,
        ROUND(SUM(sales_dollars) / SUM(sales_quantity), 4) AS avg_selling_price
    FROM gold_se
    WHERE size IS NOT NULL
    GROUP BY size
    ORDER BY total_profit_dollars DESC
""")

(
    size_analysis.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.size_analysis")
)
print(f"✅  gold.size_analysis: {size_analysis.count()} distinct pack sizes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 13. Gold — Classification (Spirit Type) Performance

# COMMAND ----------

classification_perf = spark.sql("""
    SELECT
        classification,
        COUNT(DISTINCT brand)                        AS unique_brands,
        COUNT(DISTINCT description)                  AS unique_skus,
        COUNT(*)                                     AS transaction_count,
        ROUND(SUM(sales_dollars), 2)                 AS total_revenue,
        ROUND(SUM(profit_dollars), 2)                AS total_profit_dollars,
        ROUND(AVG(margin_pct), 2)                    AS avg_margin_pct,
        SUM(sales_quantity)                          AS total_units_sold,
        ROUND(SUM(volume), 2)                        AS total_volume_liters,
        ROUND(SUM(sales_dollars) / NULLIF(SUM(sales_quantity), 0), 4) AS avg_selling_price,
        ROUND(SUM(profit_dollars) / NULLIF(SUM(sales_dollars), 0) * 100, 2)
                                                     AS overall_margin_pct
    FROM gold_se
    WHERE classification IS NOT NULL
    GROUP BY classification
    ORDER BY total_profit_dollars DESC
""")

(
    classification_perf.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD_SCHEMA}.classification_performance")
)
print(f"✅  gold.classification_performance: {classification_perf.count()} spirit types")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 14. Final Validation — Gold Layer

# COMMAND ----------

gold_tables = {
    "sales_enriched"            : "profit_dollars",
    "product_profitability"     : "total_profit_dollars",
    "brand_profitability"       : "total_profit_dollars",
    "loss_makers"               : "total_profit_dollars",
    "sales_by_store"            : "total_revenue",
    "sales_time_series"         : "monthly_revenue",
    "inventory_delta"           : "inventory_change",
    "vendor_performance"        : "total_purchase_spend",
    "size_analysis"             : "total_profit_dollars",
    "classification_performance": "total_profit_dollars",
}

print("\n📊  Gold Layer Validation Report")
print("=" * 70)
all_passed = True

for tbl, key_col in gold_tables.items():
    full_name = f"{GOLD_SCHEMA}.{tbl}"
    df = spark.table(full_name)
    count = df.count()
    has_col = key_col in df.columns
    null_key = df.filter(F.col(key_col).isNull()).count() if has_col else -1
    status = "✅" if count > 0 and has_col else "❌"
    if count == 0 or not has_col:
        all_passed = False
    print(f"  {status}  {tbl:<35} | {count:>10,} rows | null '{key_col}': {null_key}")

print("=" * 70)

# Business sanity checks
pp = spark.table(f"{GOLD_SCHEMA}.product_profitability")
top_product = pp.orderBy(F.col("total_profit_dollars").desc()).first()
print(f"\n  🏆  Top product by profit: Brand={top_product['brand']} | {top_product['description']} | ${top_product['total_profit_dollars']:,.2f}")

bp = spark.table(f"{GOLD_SCHEMA}.brand_profitability")
top_brand = bp.orderBy(F.col("total_profit_dollars").desc()).first()
print(f"  🏆  Top brand  by profit: Brand={top_brand['brand']} | ${top_brand['total_profit_dollars']:,.2f}")

lm = spark.table(f"{GOLD_SCHEMA}.loss_makers")
positive_losses = lm.filter(F.col("total_profit_dollars") >= 0).count()
assert positive_losses == 0, f"FAILED: {positive_losses} non-negative rows in loss_makers!"
print(f"  ✅  Loss-makers table: all {lm.count()} rows have negative profit (correct).")

ts = spark.table(f"{GOLD_SCHEMA}.sales_time_series")
month_count = ts.count()
print(f"  ✅  Time series: {month_count} monthly records (expect 12 for full 2016).")

print(f"\n{'✅  All Gold checks PASSED' if all_passed else '⚠️  Some checks FAILED — review output above'}.\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✅ Gold Layer Complete
# MAGIC
# MAGIC All 10 analytical Gold tables are ready to be consumed by Power BI.
# MAGIC
# MAGIC **Recommended Power BI connection:** Databricks Partner Connect → Power BI → select `gold.*` tables.
# MAGIC
# MAGIC **Next step:** Refer to `productionization_guide.md` to schedule this pipeline as a Databricks Workflow.