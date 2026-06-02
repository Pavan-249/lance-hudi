import os
import pandas as pd
import lancedb
import pyarrow as pa
from pyspark.sql import SparkSession
from sentence_transformers import SentenceTransformer

# ── paths ─────────────────────────────────────────────────────────────────────
BASE_DIR   = os.path.expanduser("~/Desktop/lance-hudi")
JAR_PATH   = f"{BASE_DIR}/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar"
HUDI_PATH  = f"{BASE_DIR}/hudi_table/wine_reviews"
LANCE_PATH = f"{BASE_DIR}/lance_db"

# ── spark session ─────────────────────────────────────────────────────────────
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

# ── read hudi snapshot ────────────────────────────────────────────────────────
df = (
    spark.read.format("hudi")
    .load(HUDI_PATH)
    .select("wine_id", "country", "variety", "points", "price", "description")
    .filter("description is not null")
)

print(f"records read from hudi: {df.count()}")

# ── convert to pandas ─────────────────────────────────────────────────────────
pdf = df.toPandas()
spark.stop()

# ── generate embeddings ───────────────────────────────────────────────────────
print("loading embedding model...")
model = SentenceTransformer("all-MiniLM-L6-v2")

print(f"embedding {len(pdf)} descriptions, this will take a few minutes...")
embeddings = model.encode(
    pdf["description"].tolist(),
    batch_size=256,
    show_progress_bar=True,
    convert_to_numpy=True,
)

print(f"embedding shape: {embeddings.shape}")

# ── clean nulls and bad values ────────────────────────────────────────────────
pdf = pdf.reset_index(drop=True)
pdf["country"]  = pdf["country"].fillna("unknown").astype(str)
pdf["variety"]  = pdf["variety"].fillna("unknown").astype(str)
pdf["points"]   = pd.to_numeric(pdf["points"], errors="coerce").fillna(0.0)
pdf["price"]    = pd.to_numeric(pdf["price"],  errors="coerce").fillna(0.0)

# ── build pyarrow table ───────────────────────────────────────────────────────
table = pa.table({
    "wine_id":     pa.array(pdf.index.tolist(),          type=pa.int64()),
    "country":     pa.array(pdf["country"].tolist(),     type=pa.string()),
    "variety":     pa.array(pdf["variety"].tolist(),     type=pa.string()),
    "points":      pa.array(pdf["points"].tolist(),      type=pa.float32()),
    "price":       pa.array(pdf["price"].tolist(),       type=pa.float32()),
    "description": pa.array(pdf["description"].tolist(), type=pa.string()),
    "vector":      pa.array(embeddings.tolist(),         type=pa.list_(pa.float32(), 384)),
})

# ── write to lancedb ──────────────────────────────────────────────────────────
db = lancedb.connect(LANCE_PATH)

if "wine_reviews" in db.table_names():
    db.drop_table("wine_reviews")

tbl = db.create_table("wine_reviews", data=table)
tbl.create_index(metric="cosine")

print(f"lancedb table written: {tbl.count_rows()} rows")
print(f"location: {LANCE_PATH}")