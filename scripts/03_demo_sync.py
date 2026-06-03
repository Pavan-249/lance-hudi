import contextlib
import logging
import os
import time
import warnings

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

import lancedb
import pandas as pd
import pyarrow as pa

from sentence_transformers import SentenceTransformer
from rich.console import Console

from checkpoint_utils import read_checkpoint, write_checkpoint, TABLE_SCHEMA

console = Console()

BASE_DIR = os.path.expanduser("~/Desktop/lance-hudi")
JAR_PATH = f"{BASE_DIR}/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar"
HUDI_PATH = f"{BASE_DIR}/hudi_table/wine_reviews"
LANCE_PATH = f"{BASE_DIR}/lance_db"
TABLE_NAME = "wine_reviews"

WINE_ID = 0

RED_DESC = (
    "A bold red wine with dark cherry, blackberry and toasted oak. "
    "Full bodied with firm tannins and a long warming finish."
)

WHITE_DESC = (
    "A crisp white wine with green apple, lemon zest and fresh herbs. "
    "Light bodied with bright acidity and a clean mineral finish."
)

RED_QUERY = "bold red wine dark cherry blackberry oak tannins"
WHITE_QUERY = "crisp white wine green apple lemon zest mineral"


@contextlib.contextmanager
def quiet():
    devnull = os.open(os.devnull, os.O_WRONLY)
    old_out = os.dup(1)
    old_err = os.dup(2)

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


def log(tag, msg, style="cyan"):
    console.print(f"[{style}][{tag}][/{style}] {msg}")


def build_spark():
    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("sync-agent")
        .master("local[*]")
        .config("spark.jars", JAR_PATH)
        .config("spark.ui.enabled", "false")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.hudi.catalog.HoodieCatalog")
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("OFF")
    return spark


def search_top(tbl, model, query):
    vec = model.encode(query, convert_to_numpy=True).tolist()
    return tbl.search(vec).limit(1).to_pandas()


def main():
    db = lancedb.connect(LANCE_PATH)
    tbl = db.open_table(TABLE_NAME)

    with quiet():
        model = SentenceTransformer("all-MiniLM-L6-v2")

    cur = tbl.search().where(f"wine_id = {WINE_ID}").limit(1).to_pandas()
    old_lance_desc = cur.iloc[0]["description"] if len(cur) else ""

    is_red = "red" in old_lance_desc.lower() and "white" not in old_lance_desc.lower()
    new_desc = WHITE_DESC if is_red else RED_DESC
    test_query = WHITE_QUERY if is_red else RED_QUERY

    console.print()
    log("INFO", "Polling Hudi timeline")

    with console.status("", spinner="dots"):
        with quiet():
            spark = build_spark()
            begin_instant = read_checkpoint(spark, HUDI_PATH)

            if begin_instant == "0":
                spark.stop()
                log("ERROR", "No checkpoint found. Run 02_embed_lance.py first.", "red")
                return

            row = (
                spark.read
                .format("hudi")
                .load(HUDI_PATH)
                .filter(f"wine_id = {WINE_ID}")
                .select(
                    "wine_id",
                    "country",
                    "variety",
                    "points",
                    "price",
                    "description",
                    "ts",
                )
                .toPandas()
                .iloc[0]
            )

            hudi_old_desc = str(row["description"])
            ts_val = int(row["ts"]) + 1000

            upd = [(
                int(row["wine_id"]),
                str(row["country"]),
                str(row["variety"]),
                float(row["points"]),
                float(row["price"]),
                new_desc,
                ts_val,
            )]

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

            (
                spark.createDataFrame(upd, TABLE_SCHEMA)
                .write
                .format("hudi")
                .options(**opts)
                .mode("append")
                .save(HUDI_PATH)
            )

            incr = (
                spark.read
                .format("hudi")
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.read.begin.instanttime", begin_instant)
                .load(HUDI_PATH)
                .select(
                    "wine_id",
                    "country",
                    "variety",
                    "points",
                    "price",
                    "description",
                    "_hoodie_commit_time",
                )
                .toPandas()
            )

    incr = incr[incr["_hoodie_commit_time"] > begin_instant]
    incr = incr[incr["wine_id"] == WINE_ID].reset_index(drop=True)

    if len(incr) == 0:
        log("ERROR", "Incremental query returned 0 rows", "red")
        spark.stop()
        return

    after_commit = str(incr["_hoodie_commit_time"].max())
    hudi_new_desc = incr.iloc[0]["description"]

    log("DETECT", f"New commit: {after_commit}", "yellow")
    log("PULL", f"{len(incr)} modified record fetched")

    console.print()
    console.print(f"  [bold]wine_{WINE_ID}[/bold]")
    console.print(f"    [red]- {hudi_old_desc[:75]}[/red]")
    console.print(f"    [green]+ {hudi_new_desc[:75]}[/green]")
    console.print()

    incr["country"] = incr["country"].fillna("unknown").astype(str)
    incr["variety"] = incr["variety"].fillna("unknown").astype(str)
    incr["points"] = pd.to_numeric(incr["points"], errors="coerce").fillna(0.0)
    incr["price"] = pd.to_numeric(incr["price"], errors="coerce").fillna(0.0)

    log("RESYNC", "Updating LanceDB")
    t0 = time.time()

    vecs = model.encode(
        incr["description"].tolist(),
        show_progress_bar=False,
        convert_to_numpy=True,
    )

    tbl.delete(f"wine_id = {WINE_ID}")

    tbl.add(pa.table({
        "wine_id": pa.array(incr["wine_id"].tolist(), type=pa.int64()),
        "country": pa.array(incr["country"].tolist(), type=pa.string()),
        "variety": pa.array(incr["variety"].tolist(), type=pa.string()),
        "points": pa.array(incr["points"].tolist(), type=pa.float32()),
        "price": pa.array(incr["price"].tolist(), type=pa.float32()),
        "description": pa.array(incr["description"].tolist(), type=pa.string()),
        "vector": pa.array(vecs.tolist(), type=pa.list_(pa.float32(), 384)),
    }))

    log("OK", f"LanceDB updated in {time.time() - t0:.2f}s", "green")

    console.print()
    log("VERIFY", "Vector search", "magenta")

    res = search_top(tbl, model, test_query)
    top = res.iloc[0]

    healthy = int(top["wine_id"]) == WINE_ID
    status = "index healthy" if healthy else "mismatch"

    console.print(
        f"    top match: wine_{int(top['wine_id'])} "
        f"distance: {top['_distance']:.3f} "
        f"({status})"
    )

    with quiet():
        write_checkpoint(spark, HUDI_PATH, TABLE_NAME, after_commit)
        spark.stop()

    log("CHECKPOINT", after_commit, "yellow")
    console.print()


if __name__ == "__main__":
    main()