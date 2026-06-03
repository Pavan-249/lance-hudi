import contextlib
import logging
import os
import warnings

os.environ["TOKENIZERS_PARALLELISM"] = "false"
warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)

from rich.console import Console

from checkpoint_utils import read_checkpoint

console = Console()

BASE_DIR = os.path.expanduser("~/Desktop/lance-hudi")
JAR_PATH = f"{BASE_DIR}/jars/hudi-spark3.5-bundle_2.12-1.0.2.jar"
HUDI_PATH = f"{BASE_DIR}/hudi_table/wine_reviews"


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


def main():
    console.print()
    log("INFO", "Polling Hudi timeline")

    with console.status("", spinner="dots"):
        with quiet():
            from pyspark.sql import SparkSession

            spark = (
                SparkSession.builder
                .appName("poll-agent")
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

            begin_instant = read_checkpoint(spark, HUDI_PATH)

            if begin_instant == "0":
                spark.stop()
                log("ERROR", "No checkpoint found. Run 02_embed_lance.py first.", "red")
                return

            incr = (
                spark.read
                .format("hudi")
                .option("hoodie.datasource.query.type", "incremental")
                .option("hoodie.datasource.read.begin.instanttime", begin_instant)
                .load(HUDI_PATH)
                .select("_hoodie_commit_time")
                .toPandas()
            )

            spark.stop()

    incr = incr[incr["_hoodie_commit_time"] > begin_instant]

    log("CHECKPOINT", begin_instant, "yellow")

    if len(incr) == 0:
        log("OK", "0 new records. LanceDB is in sync.", "green")
    else:
        log("WARN", f"{len(incr)} new records found. Run 03_demo_sync.py.", "yellow")

    console.print()


if __name__ == "__main__":
    main()