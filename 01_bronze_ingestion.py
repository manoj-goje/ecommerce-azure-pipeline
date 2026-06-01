# =============================================================================
# 01_bronze_ingestion.py
# E-Commerce Sales Analytics Pipeline — Bronze Layer
# Ingests raw CSV/JSON data from ADLS Gen2 into Delta Lake (Bronze)
# =============================================================================

# Databricks Widgets (replace with your values or use ADF parameters)
dbutils.widgets.text("storage_account", "yourstorageaccount")
dbutils.widgets.text("container", "ecommerce-raw")
dbutils.widgets.text("bronze_path", "/mnt/bronze")

STORAGE_ACCOUNT = dbutils.widgets.get("storage_account")
CONTAINER        = dbutils.widgets.get("container")
BRONZE_PATH      = dbutils.widgets.get("bronze_path")

# Mount ADLS Gen2 (run once; comment out if already mounted)
configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id":     dbutils.secrets.get("kv-scope", "sp-client-id"),
    "fs.azure.account.oauth2.client.secret": dbutils.secrets.get("kv-scope", "sp-client-secret"),
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{dbutils.secrets.get('kv-scope','tenant-id')}/oauth2/token",
}

dbutils.fs.mount(
    source=f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
    mount_point="/mnt/raw",
    extra_configs=configs,
)

# -----------------------------------------------------------------------------
# Read raw orders CSV from ADLS Gen2
# -----------------------------------------------------------------------------
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

spark = SparkSession.builder.appName("Bronze_Ingestion").getOrCreate()

raw_orders_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/raw/orders/")
)

raw_customers_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/mnt/raw/customers/")
)

raw_products_df = (
    spark.read
    .option("multiline", "true")
    .json("/mnt/raw/products/")
)

# Add audit columns
def add_audit_cols(df, source_name):
    return (
        df
        .withColumn("_ingestion_timestamp", current_timestamp())
        .withColumn("_source_file",         input_file_name())
        .withColumn("_source_name",         lit(source_name))
    )

bronze_orders    = add_audit_cols(raw_orders_df,    "orders")
bronze_customers = add_audit_cols(raw_customers_df, "customers")
bronze_products  = add_audit_cols(raw_products_df,  "products")

# -----------------------------------------------------------------------------
# Write to Bronze Delta Lake (append — keeps full history)
# -----------------------------------------------------------------------------
bronze_orders.write.format("delta").mode("append").save(f"{BRONZE_PATH}/orders")
bronze_customers.write.format("delta").mode("append").save(f"{BRONZE_PATH}/customers")
bronze_products.write.format("delta").mode("append").save(f"{BRONZE_PATH}/products")

print("✅ Bronze ingestion complete")
print(f"   Orders    : {bronze_orders.count():,} rows")
print(f"   Customers : {bronze_customers.count():,} rows")
print(f"   Products  : {bronze_products.count():,} rows")
