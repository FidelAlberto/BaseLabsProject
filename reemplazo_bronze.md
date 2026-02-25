🔐 CELL 0 — ADLS Gen2 Authentication (Storage Account Key)
# ============================================================
# CELL 0 — Azure Data Lake Gen2 Authentication
# ============================================================
# This cell configures authentication so Databricks can access
# the ADLS Gen2 account using a Storage Account Key.
#
# NOTE:
# - This approach is acceptable for labs and prototypes.
# - For production, Azure AD OAuth (Service Principal) is preferred.
# - This cell MUST be executed before any abfss path is accessed.

spark.conf.set(
    "fs.azure.account.key.anniedatalake123.dfs.core.windows.net",
    "<PASTE_STORAGE_ACCOUNT_KEY_1_HERE>"
)
🟦 CELL 1 — Azure Data Lake Base Paths
# ============================================================
# CELL 1 — Azure Data Lake Gen2 Base Paths
# ============================================================
# Centralized path definitions to:
# - Avoid hardcoded paths
# - Enable reuse across Bronze / Silver / Gold
# - Allow storage account changes without code refactoring

container_name = "annie-data"
storage_account = "anniedatalake123"

base_path = f"abfss://{container_name}@{storage_account}.dfs.core.windows.net/"
raw_path = base_path + "raw/"
bronze_path = base_path + "bronze/"
🟦 CELL 2 — Validate RAW Zone Accessibility
# ============================================================
# CELL 2 — Validate RAW Zone Accessibility
# ============================================================
# This step confirms:
# - Authentication is correctly configured
# - RAW folder is reachable
# - CSV files are visible before ingestion

dbutils.fs.ls(raw_path)

✔️ Expected: 6 CSV files listed

🟦 CELL 3 — Generic CSV Reader Function
# ============================================================
# CELL 3 — Generic CSV Reader Function
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
🟦 CELL 4 — Load RAW CSV Files into DataFrames
# ============================================================
# CELL 4 — Load RAW CSV Files into DataFrames
# ============================================================
# Each CSV file is loaded into its own DataFrame.
# This preserves lineage and simplifies debugging.

sales_df = read_csv("SalesFINAL12312016.csv")
purchases_df = read_csv("PurchasesFINAL12312016.csv")
prices_df = read_csv("2017PurchasePricesDec.csv")
begin_inventory_df = read_csv("BegInvFINAL12312016.csv")
end_inventory_df = read_csv("EndInvFINAL12312016.csv")
invoices_df = read_csv("InvoicePurchases12312016.csv")
🟦 CELL 5 — Data Inspection & Schema Validation
# ============================================================
# CELL 5 — Data Inspection & Schema Validation
# ============================================================
# Visual inspection ensures:
# - Columns are correctly parsed
# - Data types are reasonable
# - No malformed rows or parsing issues

display(sales_df)
display(purchases_df)
display(prices_df)

📌 Important step to highlight in interviews

🟦 CELL 6 — Bronze Delta Writer Function
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
🟦 CELL 7 — Persist DataFrames to Bronze Layer
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
🟦 CELL 8 — Validate Bronze Folder Structure
# ============================================================
# CELL 8 — Validate Bronze Folder Structure
# ============================================================
# Confirms:
# - Delta folders were created
# - _delta_log exists for each dataset
# - Parquet files are present

dbutils.fs.ls(bronze_path)
🟦 CELL 9 — Validate Delta Read from Bronze
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