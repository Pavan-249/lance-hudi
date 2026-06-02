# lance-hudi

Keeps a LanceDB vector index in sync with a mutable Apache Hudi table. When a record changes in Hudi, the sync detects it via an incremental query, drops the stale embedding from LanceDB, and inserts a fresh one. No full table scans. No index rebuilds.

The dataset is 130k wine reviews from Kaggle. Structured fields sit in Hudi. Semantic embeddings of each description sit in LanceDB.

## Architecture

```
                                today

  ┌──────────────────────┐                     ┌──────────────────────┐
  │     Apache Hudi      │                     │       LanceDB        │
  │  (source of truth)   │ ── 03_demo_sync ──▶ │   (derived index)    │
  │  COW table, Parquet  │        .py          │  384-dim embeddings  │
  └──────────────────────┘                     └──────────────────────┘
          incremental pull ─── re-embed ─── delete + insert


                     RFC-100 + RFC-102  (in progress)

  ┌──────────────────────────────────────────────────────────────────┐
  │                            Apache Hudi                            │
  │   Lance file format (RFC-100)  +  native vector search (RFC-102)  │
  │      vectors live inside the table, the sync script is gone       │
  └──────────────────────────────────────────────────────────────────┘
```

## Demo

```
[INFO] Polling Hudi timeline for new commits...
[DETECT] New commit detected: 20260602...
[PULL] Executing Hudi incremental pull...
[OK] 1 modified record fetched (0 full table scans)

  Update detected: record 'wine_0'
    - A bold red wine with dark cherry, blackberry and toasted oak...
    + A crisp white wine with green apple, lemon zest and fresh herbs...

[RESYNC] Initiating LanceDB resync
    > generating 384-dim embedding for updated description
    > DELETE id 'wine_0' from vector index
    > INSERT new embedding for id 'wine_0' into vector index
[OK] Resync completed in 0.34s

[VERIFY] System verification
    > search(query="crisp white wine green apple lemon zest mineral")
    > top match: wine_0  distance: 0.145  (index healthy)
```

## Setup

Prerequisites: Python 3.11, Java 11 (required by PySpark).

Install dependencies:

```bash
pip install pyspark==3.5.3 lancedb sentence-transformers pyarrow pandas rich
```

Download the Hudi jar:

```bash
mkdir -p jars
curl -L -o jars/hudi-spark3.5-bundle_2.12-1.0.2.jar \
  https://repo1.maven.org/maven2/org/apache/hudi/hudi-spark3.5-bundle_2.12/1.0.2/hudi-spark3.5-bundle_2.12-1.0.2.jar
```

Dataset: download `winemag-data-130k-v2.csv` from Kaggle and place it at `data/winemag-data-130k-v2.csv`.

## Run order

```bash
python scripts/01_ingest_hudi.py  # reads the CSV and writes 130k records to a Hudi COW table
python scripts/02_embed_lance.py  # reads the Hudi snapshot, embeds all descriptions with all-MiniLM-L6-v2, writes to LanceDB
python scripts/03_demo_sync.py    # mutates a record in Hudi, detects it via incremental pull, resyncs LanceDB, verifies with semantic search
```

## How the sync works

Hudi stamps every write with a commit timestamp on its timeline. The sync reads a checkpoint: the last commit it processed. It runs an incremental query that returns only records committed after that timestamp. For each changed record, it re-embeds the new description. It then deletes the stale vector from LanceDB by record ID and inserts the fresh embedding. Finally, it saves the new commit timestamp as the next checkpoint. The next run picks up from there. Only records that changed are touched.

## Why Hudi

Hudi is built for change data capture. Every write lands on its timeline as a commit with a monotonic timestamp. An incremental query returns only the records written after a given commit. That single primitive is the whole sync. No diffing. No separate change table. No full scan to find what moved. Hudi already tracks it.

This is what makes the index cheap to maintain. The sync re-embeds only the rows that changed. Cost scales with the size of the change, not the size of the table. On 130k rows that gap is small. On hundreds of millions of rows it is the difference between a feasible sync and an impossible one.

## What's next

Two RFCs in the Apache Hudi project will fold the vector index back into the table. [RFC-100](https://github.com/apache/hudi/issues/14127) adds Lance as a first-class file format in Hudi, so vectors store in the same table as the structured fields. [RFC-102](https://github.com/apache/hudi/issues/14219) adds native vector similarity search on Hudi tables.

Once both land, this architecture collapses. There is no second store to keep consistent. The CSV still ingests into Hudi. The descriptions embed into a vector column on the same table. Search runs against Hudi directly. The incremental-pull, delete, re-insert loop in `03_demo_sync.py` goes away. The sync script was only ever standing in for a feature Hudi is about to grow. When it ships, you delete the script.

## Limitations

PySpark is required for all writes. `hudi-rs` and Daft both cap out at Hudi table version 6. This table is version 8 (written by Hudi 1.0.2). I have not found a non-Spark path for writes at this table version.

LanceDB has no in-place vector update. The sync uses delete-then-insert. There is a brief window between the delete and the insert where a changed record has no vector in the index.

The sync is per-run, not a daemon. Each invocation of `03_demo_sync.py` processes one batch of changes and exits. A production version would poll the timeline on an interval.
