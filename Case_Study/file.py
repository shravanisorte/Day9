from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit, monotonically_increasing_id

spark = SparkSession.builder \
    .appName("SCD Type-2 Implementation") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Step 1: Source (Incoming) Data
source_df = spark.createDataFrame([
    (1, "Alice", "New York", "Gold"),
    (2, "Bob", "San Francisco", "Silver"),
    (3, "Charlie", "Los Angeles", "Platinum")
], ["CustomerID", "Name", "Address", "LoyaltyTier"])

# Step 2: Load Existing DimCustomer Table
existing_df = spark.read.format("delta").load(r"C:/Users/shravani.raju/Desktop/Day9/Case_Study/dim_customer")

# Step 3: Detect Changes
joined_df = source_df.alias("src").join(
    existing_df.filter("IsCurrent = true").alias("tgt"),
    on="CustomerID", how="left"
)

changed_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("src.*")

# Step 4: Expire Old Records
expired_df = joined_df.filter(
    (col("src.Address") != col("tgt.Address")) |
    (col("src.LoyaltyTier") != col("tgt.LoyaltyTier"))
).select("tgt.CustomerSK", "tgt.CustomerID", "tgt.Name", "tgt.Address", "tgt.LoyaltyTier") \
 .withColumn("EndDate", current_date()) \
 .withColumn("IsCurrent", lit(False))

# Step 5: Insert New Versioned Records
new_version_df = changed_df \
    .withColumn("CustomerSK", monotonically_increasing_id()) \
    .withColumn("StartDate", current_date()) \
    .withColumn("EndDate", lit(None).cast("date")) \
    .withColumn("IsCurrent", lit(True))

# Step 6: Merge Final Records
unchanged_df = existing_df.filter("IsCurrent = true").join(
    expired_df.select("CustomerSK"), on="CustomerSK", how="left_anti")

final_df = unchanged_df.unionByName(expired_df).unionByName(new_version_df)

# Step 7: Write Back
final_df.write.format("delta").mode("overwrite").save(r"C:/Users/shravani.raju/Desktop/Day9/Case_Study/dim_customer")

spark.stop()
