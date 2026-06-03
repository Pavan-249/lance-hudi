from pyspark.sql.types import (
    StructType,
    StructField,
    LongType,
    StringType,
    DoubleType,
)

BOOKMARK_KEY = "lance.sync.bookmark"
COMMITMETA_PREFIX = "lance.sync"

TABLE_SCHEMA = StructType([
    StructField("wine_id", LongType(), False),
    StructField("country", StringType(), True),
    StructField("variety", StringType(), True),
    StructField("points", DoubleType(), True),
    StructField("price", DoubleType(), True),
    StructField("description", StringType(), True),
    StructField("ts", LongType(), False),
])


def _commits_timeline(spark, hudi_path):
    jvm = spark._jvm
    storage_conf = jvm.org.apache.hudi.storage.hadoop.HadoopStorageConfiguration(
        spark._jsc.hadoopConfiguration()
    )
    meta_client = (
        jvm.org.apache.hudi.common.table.HoodieTableMetaClient.builder()
        .setConf(storage_conf)
        .setBasePath(hudi_path)
        .build()
    )
    return meta_client.getActiveTimeline().getCommitsTimeline().filterCompletedInstants()


def latest_commit_instant(spark, hudi_path):
    timeline = _commits_timeline(spark, hudi_path)
    last = timeline.lastInstant()
    return last.get().requestedTime() if last.isPresent() else "0"


def read_checkpoint(spark, hudi_path):
    timeline = _commits_timeline(spark, hudi_path)

    for instant in reversed(list(timeline.getInstants().toArray())):
        metadata = timeline.readCommitMetadata(instant)
        bookmark = metadata.getMetadata(BOOKMARK_KEY)

        if bookmark:
            return str(bookmark)

    return "0"


def write_checkpoint(spark, hudi_path, table_name, synced_through):
    opts = {
        "hoodie.table.name": table_name,
        "hoodie.datasource.write.table.type": "COPY_ON_WRITE",
        "hoodie.datasource.write.operation": "upsert",
        "hoodie.datasource.write.recordkey.field": "wine_id",
        "hoodie.datasource.write.precombine.field": "ts",
        "hoodie.datasource.write.partitionpath.field": "country",
        "hoodie.datasource.write.hive_style_partitioning": "true",
        "hoodie.upsert.shuffle.parallelism": "1",
        "hoodie.datasource.write.commitmeta.key.prefix": COMMITMETA_PREFIX,
        BOOKMARK_KEY: str(synced_through),
    }

    (
        spark.createDataFrame([], TABLE_SCHEMA)
        .write
        .format("hudi")
        .options(**opts)
        .mode("append")
        .save(hudi_path)
    )