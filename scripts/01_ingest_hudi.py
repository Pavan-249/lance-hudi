import os
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

# ── paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.expanduser("~/Desktop/lance-hudi")
JAR_PATH   = f"{BASE_DIR}/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar"
CSV_PATH   = f"{BASE_DIR}/data/winemag-data-130k-v2.csv"
HUDI_PATH  = f"{BASE_DIR}/hudi_table/wine_reviews"
TABLE_NAME = "wine_reviews"

# ── clean any prior table so there is no schema history to evolve against ─────
if os.path.exists(HUDI_PATH):
    shutil.rmtree(HUDI_PATH)
    print(f"removed existing table at {HUDI_PATH}")

# ── spark session ─────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("lance-hudi-ingest")
    .master("local[*]")
    .config("spark.jars", JAR_PATH)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ── read csv ──────────────────────────────────────────────────────────────────
raw = spark.read.csv(CSV_PATH, header=True, inferSchema=True)

# ── select ONLY the columns we use, cast explicitly ──────────────────────────
# this avoids schema drift from messy columns like winery/taster_name
df = (
    raw
    .withColumnRenamed("_c0", "wine_id")
    .select(
        col("wine_id").cast("long").alias("wine_id"),
        col("country").cast("string").alias("country"),
        col("variety").cast("string").alias("variety"),
        col("points").cast("double").alias("points"),
        col("price").cast("double").alias("price"),
        col("description").cast("string").alias("description"),
    )
    .withColumn("ts", current_timestamp().cast("long"))
    .filter(col("description").isNotNull())
    .filter(col("wine_id").isNotNull())
    .filter(col("country").isNotNull())
)

print(f"total records: {df.count()}")
df.printSchema()

# ── hudi write options ────────────────────────────────────────────────────────
hudi_options = {
    "hoodie.table.name":                               TABLE_NAME,
    "hoodie.datasource.write.table.type":              "COPY_ON_WRITE",
    "hoodie.datasource.write.operation":               "bulk_insert",
    "hoodie.datasource.write.recordkey.field":         "wine_id",
    "hoodie.datasource.write.precombine.field":        "ts",
    "hoodie.datasource.write.partitionpath.field":     "country",
    "hoodie.datasource.write.hive_style_partitioning":  "true",
    "hoodie.insert.shuffle.parallelism":               "2",
    "hoodie.upsert.shuffle.parallelism":               "2",
}

(
    df.write
    .format("hudi")
    .options(**hudi_options)
    .mode("overwrite")
    .save(HUDI_PATH)
)

print(f"hudi table written to {HUDI_PATH}")

# ── verify ────────────────────────────────────────────────────────────────────
verify_df = spark.read.format("hudi").load(HUDI_PATH)
print(f"rows written: {verify_df.count()}")
verify_df.select("wine_id", "country", "variety", "points", "price", "description").show(5, truncate=80)

spark.stop()