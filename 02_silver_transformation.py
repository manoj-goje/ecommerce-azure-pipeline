# =============================================================================
# 02_silver_transformation.py
# E-Commerce Sales Analytics Pipeline — Silver Layer
# Cleans, deduplicates, and enriches Bronze data → Delta Lake (Silver)
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, upper, lower, when, coalesce, lit,
    to_date, to_timestamp, datediff, current_date,
    regexp_replace, round as spark_round,
    current_timestamp
)
from delta.tables import DeltaTable

spark = SparkSession.builder.appName("Silver_Transformation").getOrCreate()

BRONZE_PATH = "/mnt/bronze"
SILVER_PATH = "/mnt/silver"

# =============================================================================
# ORDERS — clean & validate
# =============================================================================
bronze_orders = spark.read.format("delta").load(f"{BRONZE_PATH}/orders")

silver_orders = (
    bronze_orders
    # Drop rows missing critical fields
    .dropna(subset=["order_id", "customer_id", "order_date", "total_amount"])
    # Deduplicate on order_id (keep latest ingestion)
    .dropDuplicates(["order_id"])
    # Standardise types
    .withColumn("order_date",    to_date(col("order_date"), "yyyy-MM-dd"))
    .withColumn("total_amount",  spark_round(col("total_amount").cast("double"), 2))
    .withColumn("status",        upper(trim(col("status"))))
    # Derive useful columns
    .withColumn("order_year",    col("order_date").cast("string").substr(1, 4))
    .withColumn("order_month",   col("order_date").cast("string").substr(6, 2))
    .withColumn("days_since_order", datediff(current_date(), col("order_date")))
    # Flag suspicious amounts
    .withColumn("is_high_value", when(col("total_amount") > 1000, True).otherwise(False))
    # Audit
    .withColumn("_silver_timestamp", current_timestamp())
)

# =============================================================================
# CUSTOMERS — clean & enrich
# =============================================================================
bronze_customers = spark.read.format("delta").load(f"{BRONZE_PATH}/customers")

silver_customers = (
    bronze_customers
    .dropna(subset=["customer_id", "email"])
    .dropDuplicates(["customer_id"])
    .withColumn("email",        lower(trim(col("email"))))
    .withColumn("full_name",    trim(col("full_name")))
    .withColumn("phone",        regexp_replace(col("phone"), r"[^0-9+]", ""))
    .withColumn("country_code", upper(trim(coalesce(col("country_code"), lit("IN")))))
    .withColumn("_silver_timestamp", current_timestamp())
)

# =============================================================================
# PRODUCTS — clean & categorise
# =============================================================================
bronze_products = spark.read.format("delta").load(f"{BRONZE_PATH}/products")

silver_products = (
    bronze_products
    .dropna(subset=["product_id", "product_name", "price"])
    .dropDuplicates(["product_id"])
    .withColumn("product_name", trim(col("product_name")))
    .withColumn("category",     upper(trim(col("category"))))
    .withColumn("price",        spark_round(col("price").cast("double"), 2))
    # Price tier
    .withColumn("price_tier",
        when(col("price") < 500,  "BUDGET")
        .when(col("price") < 2000, "MID")
        .otherwise("PREMIUM")
    )
    .withColumn("_silver_timestamp", current_timestamp())
)

# =============================================================================
# Write to Silver Delta Lake (overwrite — idempotent silver layer)
# =============================================================================
silver_orders.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_PATH}/orders")
silver_customers.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_PATH}/customers")
silver_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_PATH}/products")

print("✅ Silver transformation complete")
print(f"   Orders    : {silver_orders.count():,} rows")
print(f"   Customers : {silver_customers.count():,} rows")
print(f"   Products  : {silver_products.count():,} rows")
