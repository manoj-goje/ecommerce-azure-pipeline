# =============================================================================
# 03_gold_aggregation.py
# E-Commerce Sales Analytics Pipeline — Gold Layer
# Builds BI-ready aggregated KPI tables → Synapse Analytics
# =============================================================================

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, sum as spark_sum, count, countDistinct,
    avg, max as spark_max, min as spark_min,
    round as spark_round, rank, desc, current_timestamp
)
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("Gold_Aggregation").getOrCreate()

SILVER_PATH = "/mnt/silver"
GOLD_PATH   = "/mnt/gold"

# Load Silver tables
orders    = spark.read.format("delta").load(f"{SILVER_PATH}/orders")
customers = spark.read.format("delta").load(f"{SILVER_PATH}/customers")
products  = spark.read.format("delta").load(f"{SILVER_PATH}/products")

# Join orders ↔ products (assuming order_items bridge table)
order_items = spark.read.format("delta").load(f"{SILVER_PATH}/order_items")

enriched = (
    order_items
    .join(orders,    "order_id",    "left")
    .join(customers, "customer_id", "left")
    .join(products,  "product_id",  "left")
)

# =============================================================================
# GOLD TABLE 1: Monthly Revenue Summary
# =============================================================================
monthly_revenue = (
    enriched
    .groupBy("order_year", "order_month")
    .agg(
        spark_sum("total_amount").alias("total_revenue"),
        count("order_id").alias("total_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        spark_max("total_amount").alias("max_order_value"),
    )
    .orderBy("order_year", "order_month")
    .withColumn("_gold_timestamp", current_timestamp())
)

# =============================================================================
# GOLD TABLE 2: Top Products by Revenue
# =============================================================================
window_rank = Window.orderBy(desc("product_revenue"))

top_products = (
    enriched
    .groupBy("product_id", "product_name", "category", "price_tier")
    .agg(
        spark_sum("quantity").alias("units_sold"),
        spark_round(spark_sum(col("quantity") * col("price")), 2).alias("product_revenue"),
        countDistinct("order_id").alias("order_count"),
    )
    .withColumn("revenue_rank", rank().over(window_rank))
    .filter(col("revenue_rank") <= 50)
    .withColumn("_gold_timestamp", current_timestamp())
)

# =============================================================================
# GOLD TABLE 3: Customer Lifetime Value (CLV) Segments
# =============================================================================
clv = (
    enriched
    .groupBy("customer_id", "full_name", "email", "country_code")
    .agg(
        spark_round(spark_sum("total_amount"), 2).alias("lifetime_value"),
        count("order_id").alias("total_orders"),
        spark_round(avg("total_amount"), 2).alias("avg_order_value"),
        spark_min("order_date").alias("first_order_date"),
        spark_max("order_date").alias("last_order_date"),
    )
    .withColumn("clv_segment",
        __import__("pyspark.sql.functions", fromlist=["when"]).when(col("lifetime_value") >= 10000, "PLATINUM")
        .when(col("lifetime_value") >= 5000, "GOLD")
        .when(col("lifetime_value") >= 1000, "SILVER")
        .otherwise("BRONZE")
    )
    .withColumn("_gold_timestamp", current_timestamp())
)

# =============================================================================
# Write to Gold Delta Lake (partitioned for fast BI queries)
# =============================================================================
(monthly_revenue
 .write.format("delta")
 .mode("overwrite")
 .partitionBy("order_year", "order_month")
 .save(f"{GOLD_PATH}/monthly_revenue"))

(top_products
 .write.format("delta")
 .mode("overwrite")
 .partitionBy("category")
 .save(f"{GOLD_PATH}/top_products"))

(clv
 .write.format("delta")
 .mode("overwrite")
 .partitionBy("clv_segment")
 .save(f"{GOLD_PATH}/customer_clv"))

# Z-Order for faster BI queries on Gold tables
spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}/monthly_revenue` ZORDER BY (order_year, order_month)")
spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}/top_products` ZORDER BY (revenue_rank)")
spark.sql(f"OPTIMIZE delta.`{GOLD_PATH}/customer_clv` ZORDER BY (lifetime_value)")

print("✅ Gold aggregation complete")
print(f"   Monthly Revenue rows : {monthly_revenue.count():,}")
print(f"   Top Products rows    : {top_products.count():,}")
print(f"   CLV Segments rows    : {clv.count():,}")
