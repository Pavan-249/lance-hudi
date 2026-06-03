import os
import shutil

import lancedb
import pandas as pd
import pyarrow as pa

from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer

from checkpoint_utils import latest_commit_instant, write_checkpoint

BASE_DIR = os.path.expanduser("~/Desktop/lance-hudi")
JAR_PATH = f"{BASE_DIR}/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar"
HUDI_PATH = f"{BASE_DIR}/hudi_table/wine_reviews"
LANCE_PATH = f"{BASE_DIR}/lance_db"
TABLE_NAME = "wine_reviews"

spark = (
    SparkSession.builder
    .appName("lance-hudi-embed")
    .master("local[*]")
    .config("spark.jars", JAR_PATH)
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
    .config("spark.driver.memory", "4g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

ingest_commit = latest_commit_instant(spark, HUDI_PATH)

df = (
    spark.read
    .format("hudi")
    .load(HUDI_PATH)
    .select("wine_id", "country", "variety", "points", "price", "description")
    .filter("description is not null")
)

pdf = df.toPandas()

print(f"embedding rows: {len(pdf)}")

model = SentenceTransformer("all-MiniLM-L6-v2")

embeddings = model.encode(
    pdf["description"].tolist(),
    batch_size=256,
    show_progress_bar=True,
    convert_to_numpy=True,
)

pdf = pdf.reset_index(drop=True)
pdf["country"] = pdf["country"].fillna("unknown").astype(str)
pdf["variety"] = pdf["variety"].fillna("unknown").astype(str)
pdf["points"] = pd.to_numeric(pdf["points"], errors="coerce").fillna(0.0)
pdf["price"] = pd.to_numeric(pdf["price"], errors="coerce").fillna(0.0)

table = pa.table({
    "wine_id": pa.array(pdf["wine_id"].tolist(), type=pa.int64()),
    "country": pa.array(pdf["country"].tolist(), type=pa.string()),
    "variety": pa.array(pdf["variety"].tolist(), type=pa.string()),
    "points": pa.array(pdf["points"].tolist(), type=pa.float32()),
    "price": pa.array(pdf["price"].tolist(), type=pa.float32()),
    "description": pa.array(pdf["description"].tolist(), type=pa.string()),
    "vector": pa.array(embeddings.tolist(), type=pa.list_(pa.float32(), 384)),
})

if os.path.exists(LANCE_PATH):
    shutil.rmtree(LANCE_PATH)

db = lancedb.connect(LANCE_PATH)
tbl = db.create_table(TABLE_NAME, data=table)
tbl.create_index(metric="cosine")

write_checkpoint(spark, HUDI_PATH, TABLE_NAME, ingest_commit)

print(f"lancedb rows: {tbl.count_rows()}")
print(f"checkpoint: {ingest_commit}")

spark.stop()