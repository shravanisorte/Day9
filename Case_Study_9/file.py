from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, lit, monotonically_increasing_id
from delta import configure_spark_with_delta_pip
import os

# Set Python environment explicitly
os.environ["PYSPARK_PYTHON"] = "C:/Users/shravani.raju/AppData/Local/Programs/Python/Python310/python.exe"
os.environ["PYSPARK_DRIVER_PYTHON"] = "C:/Users/shravani.raju/AppData/Local/Programs/Python/Python310/python.exe"

# Initialize Spark with Delta support
builder = SparkSession.builder \
    .appName("GDPR_Compliance_with_Delta") \
    .master("local[*]") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# Step 1: Incoming data
source_df = spark.createDataFrame([
    (1, "Alice", "New York", "Gold"),
    (2, "Bob", "San Francisco", "Silver"),
    (3, "Charlie", "Los Angeles", "Platinum")
], ["CustomerID", "Name", "Address", "LoyaltyTier"])

# Define path
read_path = r"C:\Users\shravani.raju\Desktop\Day8\Case_Study_9\result"
write_path = r"C:\Users\shravani.raju\Desktop\Day8\Case_Study_9\result"

# Step 2: Load Existing DimCustomer Table
if os.path.exists(write_path):
    existing_df = spark.read.format("delta").load(read_path)

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

else:
    # If path doesn’t exist, initialize full version with surrogate key
    final_df = source_df.withColumn("CustomerSK", monotonically_increasing_id()) \
                        .withColumn("StartDate", current_date()) \
                        .withColumn("EndDate", lit(None).cast("date")) \
                        .withColumn("IsCurrent", lit(True))

# Step 7: Write Back as Delta
final_df.write.format("delta").mode("overwrite").save(write_path)

spark.stop()
