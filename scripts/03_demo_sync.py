import os
import time
import warnings
import logging
import contextlib

os.environ["HF_HUB_DISABLE_PROGRESS_BARS"] = "1"
os.environ["HF_HUB_DISABLE_TELEMETRY"] = "1"
os.environ["HF_HUB_DISABLE_IMPLICIT_TOKEN"] = "1"
os.environ["TRANSFORMERS_VERBOSITY"] = "error"
os.environ["TRANSFORMERS_NO_ADVISORY_WARNINGS"] = "1"
os.environ["TOKENIZERS_PARALLELISM"] = "false"
warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)
logging.getLogger("huggingface_hub").setLevel(logging.ERROR)
logging.getLogger("transformers").setLevel(logging.ERROR)
logging.getLogger("sentence_transformers").setLevel(logging.ERROR)

import pandas as pd
import lancedb
import pyarrow as pa
from sentence_transformers import SentenceTransformer

from rich.console import Console
console = Console()

BASE_DIR   = os.path.expanduser("~/Desktop/lance-hudi")
JAR_PATH   = f"{BASE_DIR}/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar"
HUDI_PATH  = f"{BASE_DIR}/hudi_table/wine_reviews"
LANCE_PATH = f"{BASE_DIR}/lance_db"
TABLE_NAME = "wine_reviews"

WINE_ID = 0
RED_DESC = ("A bold red wine with dark cherry, blackberry and toasted oak. "
            "Full bodied with firm tannins and a long warming finish.")
WHITE_DESC = ("A crisp white wine with green apple, lemon zest and fresh herbs. "
              "Light bodied with bright acidity and a clean mineral finish.")
RED_QUERY   = "bold red wine dark cherry blackberry oak tannins"
WHITE_QUERY = "crisp white wine green apple lemon zest mineral"


@contextlib.contextmanager
def quiet():
    devnull = os.open(os.devnull, os.O_WRONLY)
    old_out, old_err = os.dup(1), os.dup(2)
    try:
        os.dup2(devnull, 1)
        os.dup2(devnull, 2)
        yield
    finally:
        os.dup2(old_out, 1)
        os.dup2(old_err, 2)
        os.close(devnull)
        os.close(old_out)
        os.close(old_err)


def search_top(tbl, model, query, limit=1):
    vec = model.encode(query, convert_to_numpy=True).tolist()
    res = tbl.search(vec).limit(limit).to_pandas()
    return res


def log(tag, msg, tag_style="cyan"):
    console.print(f"[{tag_style}][{tag}][/{tag_style}] {msg}")


def main():
    db    = lancedb.connect(LANCE_PATH)
    tbl   = db.open_table(TABLE_NAME)
    with quiet():
        model = SentenceTransformer("all-MiniLM-L6-v2")

    # decide flip direction from current state
    cur = tbl.search().where(f"wine_id = {WINE_ID}").limit(1).to_pandas()
    old_desc_lance = cur.iloc[0]["description"] if len(cur) else ""
    is_red = "red" in old_desc_lance.lower() and "white" not in old_desc_lance.lower()
    new_desc  = WHITE_DESC if is_red else RED_DESC
    test_query = WHITE_QUERY if is_red else RED_QUERY

    console.print()
    log("INFO", "Polling Hudi timeline for new commits...")

    # bring spark up quietly
    with console.status("", spinner="dots"):
        with quiet():
            from pyspark.sql import SparkSession
            from pyspark.sql.types import (StructType, StructField, LongType,
                                            StringType, DoubleType)
            spark = (
                SparkSession.builder.appName("sync-agent").master("local[*]")
                .config("spark.jars", JAR_PATH)
                .config("spark.ui.enabled", "false")
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
                .config("spark.driver.memory", "4g")
                .getOrCreate()
            )
            spark.sparkContext.setLogLevel("OFF")

            # capture latest commit and current value before the change
            commits_before = (
                spark.read.format("hudi").load(HUDI_PATH)
                .select("_hoodie_commit_time").distinct()
                .orderBy("_hoodie_commit_time").collect()
            )
            before_commit = commits_before[-1]["_hoodie_commit_time"]
            row = (
                spark.read.format("hudi").load(HUDI_PATH)
                .filter(f"wine_id = {WINE_ID}")
                .select("wine_id", "country", "variety", "points", "price", "description")
                .toPandas().iloc[0]
            )
            hudi_old_desc = str(row["description"])

            # write the change
            ts_val = int(pd.Timestamp.now().timestamp())
            schema = StructType([
                StructField("wine_id", LongType(), False),
                StructField("country", StringType(), True),
                StructField("variety", StringType(), True),
                StructField("points", DoubleType(), True),
                StructField("price", DoubleType(), True),
                StructField("description", StringType(), True),
                StructField("ts", LongType(), False),
            ])
            upd = [(int(row["wine_id"]), str(row["country"]), str(row["variety"]),
                    float(row["points"]), float(row["price"]), new_desc, ts_val)]
            opts = {
                "hoodie.table.name": TABLE_NAME,
                "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
                "hoodie.datasource.write.operation": "upsert",
                "hoodie.datasource.write.recordkey.field": "wine_id",
                "hoodie.datasource.write.precombine.field": "ts",
                "hoodie.datasource.write.partitionpath.field": "country",
                "hoodie.datasource.write.hive_style_partitioning": "true",
                "hoodie.upsert.shuffle.parallelism": "2",
            }
            (spark.createDataFrame(upd, schema)
                .write.format("hudi").options(**opts).mode("append").save(HUDI_PATH))

            # incremental pull
            incr = (
                spark.read.format("hudi")
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.read.begin.instanttime", before_commit)
                .load(HUDI_PATH)
                .select("wine_id", "country", "variety", "points", "price",
                        "description", "_hoodie_commit_time")
                .toPandas()
            )
            after_commit = incr["_hoodie_commit_time"].max() if len(incr) else before_commit
            spark.stop()

    incr = incr[incr["wine_id"] == WINE_ID].reset_index(drop=True)
    hudi_new_desc = incr.iloc[0]["description"]
    short_commit = str(after_commit)[:13]

    log("DETECT", f"New commit detected: {short_commit}", "yellow")
    log("PULL", "Executing Hudi incremental pull...")
    log("OK", f"{len(incr)} modified record fetched (0 full table scans)", "green")

    # the diff, standard git convention: removal red, addition green
    console.print()
    console.print(f"  [bold]Update detected: record 'wine_{WINE_ID}'[/bold]")
    console.print(f"    [red]- {hudi_old_desc[:75]}[/red]")
    console.print(f"    [green]+ {hudi_new_desc[:75]}[/green]")
    console.print()

    # resync
    log("RESYNC", "Initiating LanceDB resync")
    incr["country"] = incr["country"].fillna("unknown").astype(str)
    incr["variety"] = incr["variety"].fillna("unknown").astype(str)
    incr["points"]  = pd.to_numeric(incr["points"], errors="coerce").fillna(0.0)
    incr["price"]   = pd.to_numeric(incr["price"],  errors="coerce").fillna(0.0)

    t0 = time.time()
    vecs = model.encode(incr["description"].tolist(), show_progress_bar=False,
                        convert_to_numpy=True)
    console.print(f"    [white]>[/white] generating {vecs.shape[1]}-dim embedding for updated description")
    tbl.delete(f"wine_id = {WINE_ID}")
    console.print(f"    [white]>[/white] [red]DELETE[/red] id 'wine_{WINE_ID}' from vector index")
    tbl.add(pa.table({
        "wine_id":     pa.array(incr["wine_id"].tolist(),     type=pa.int64()),
        "country":     pa.array(incr["country"].tolist(),     type=pa.string()),
        "variety":     pa.array(incr["variety"].tolist(),     type=pa.string()),
        "points":      pa.array(incr["points"].tolist(),      type=pa.float32()),
        "price":       pa.array(incr["price"].tolist(),       type=pa.float32()),
        "description": pa.array(incr["description"].tolist(), type=pa.string()),
        "vector":      pa.array(vecs.tolist(),                type=pa.list_(pa.float32(), 384)),
    }))
    console.print(f"    [white]>[/white] [green]INSERT[/green] new embedding for id 'wine_{WINE_ID}' into vector index")
    log("OK", f"Resync completed in {time.time() - t0:.2f}s", "green")

    # verify
    console.print()
    log("VERIFY", "System verification", "magenta")
    console.print(f'    [white]>[/white] search(query="{test_query}")')
    res = search_top(tbl, model, test_query, limit=1)
    top = res.iloc[0]
    dist = top["_distance"]
    healthy = int(top["wine_id"]) == WINE_ID
    mark = "[green]index healthy[/green]" if healthy else "[red]mismatch[/red]"
    console.print(f"    > top match: [bold]wine_{int(top['wine_id'])}[/bold]  "
                  f"distance: {dist:.3f}  ({mark})")
    console.print()


if __name__ == "__main__":
    main()