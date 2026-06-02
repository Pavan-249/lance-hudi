# One Row Changed in Hudi. The Vector Index Resynced Itself.

Apache Hudi holds the records and LanceDB holds the embeddings. When a wine's description changes in Hudi, the embedding sitting in LanceDB is now stale, and every semantic search against it returns answers built on data that no longer exists. The usual fix is to re-embed the whole table and rebuild the index from scratch. This project takes a different path: it detects the one row that changed and re-embeds only that row.

---

## Contents

- [The problem worth solving](#the-problem-worth-solving)
- [Architecture](#architecture)
- [Why Hudi](#why-hudi)
- [The stack](#the-stack)
- [How the sync works](#how-the-sync-works)
- [The demo](#the-demo)
- [What's next](#whats-next)
- [What this is not](#what-this-is-not)

---

## The problem worth solving

A vector index is a derived artifact. You build it once from a source table, and from then on it stays correct only as long as the source holds still. Real tables do not hold still. A description gets corrected, a record gets updated, and the embedding that represented the old text is now pointing at something that changed underneath it.

The common way to deal with this is a nightly full rebuild: read every row, embed every description, and write a fresh index over the top. That is fine when the table is small, but it stops making sense as the data grows. Re-embedding a hundred million descriptions every night to fix the dozen that actually changed is a lot of compute spent on rows that never moved.

The question worth asking is narrower. When a single row changes, can you update just that row in the index and leave the rest untouched? On a Hudi table you can, and the rest of this post is how.

## Architecture

The setup has two stores with a clear hierarchy between them. Hudi is the source of truth, LanceDB is an index derived from it, and a sync script is what keeps the derived copy honest.

[ Lucidchart: Diagram 1 — see assets section ]

On the Hudi side you have the structured fields: country, variety, points, price, and the raw description text. On the LanceDB side you have a 384-dim embedding of each description, stored next to a copy of those same fields so you can filter and search in one query. Nothing connects the two stores except the sync, which runs after a write, pulls back whatever changed, and patches the index in place.

The data itself is the Kaggle wine reviews set, 130k records, where each wine carries a handful of structured columns and one block of free-text description. That description is the part that gets embedded, and it is also the field most likely to be edited or corrected later, which makes it a natural place to test what happens when the source drifts.

## Why Hudi

Hudi is built around change data capture, and that is the property this project leans on. Every write lands on the table's timeline as a commit stamped with a monotonic timestamp. Ask Hudi for an incremental query from a given commit and it hands back only the records written after that point, nothing else. There is no diffing step, no separate change-tracking table to maintain, and no full scan to figure out what moved, because Hudi already recorded exactly that when the write happened.

[ Lucidchart: Diagram 2 — Hudi commit timeline, see assets section ]

[ carbon.sh: Snippet A — see assets section ]

This is what keeps the index cheap to maintain. The sync only ever re-embeds the rows that actually changed, so the cost scales with the size of the change rather than the size of the table. On 130k wine reviews that distinction barely registers, but on a table with a hundred million rows it is the line between a sync you can run continuously and one you cannot run at all.

## The stack

The write path runs on PySpark 3.5 against a Hudi 1.0.2 copy-on-write table. LanceDB is the vector store, embeddings come from sentence-transformers running all-MiniLM-L6-v2 at 384 dimensions, and PyArrow handles moving the data into LanceDB with explicit column types. Three scripts run in order.

The first reads the wine reviews CSV and writes all 130k records into the Hudi table. The second reads that Hudi snapshot back, embeds every description, and loads the vectors into LanceDB. The third is the demo: it changes one record in Hudi, detects the change through an incremental query, resyncs LanceDB, and confirms the result with a semantic search.

## How the sync works

The mechanism is a short loop. Hudi stamps every write with a commit timestamp, and the sync holds onto a checkpoint, which is just the last commit it managed to process. On each run it asks Hudi for everything committed after that checkpoint, re-embeds the descriptions on whatever comes back, and then updates LanceDB one record at a time before saving the new commit timestamp as the next checkpoint. The following run starts from there, so no commit is ever processed twice and nothing in between is missed.

The update itself is a delete followed by an insert, which is a constraint LanceDB imposes rather than a choice I made. There is no in-place vector update, so the only way to replace an embedding is to remove the old row by its id and add the new one in its place.

[ carbon.sh: Snippet B — see assets section ]

## The demo

The demo takes wine_0, rewrites its description from a bold red to a crisp white, and then checks whether the index noticed. The terminal output walks through the whole thing.

[ carbon.sh: Snippet C — see assets section ]

The incremental pull returns one modified record and runs zero full table scans to find it. The stale vector gets deleted, the fresh one goes in, and a search for "crisp white wine green apple lemon zest mineral" comes back with wine_0 at the top and a low distance score. The index ended up consistent with Hudi without ever being rebuilt.

## What's next

This whole architecture is a bridge, and I want to be upfront about that. The only reason a sync script exists is that the vectors live in a store separate from the table they describe, and two RFCs in the Apache Hudi project are aimed squarely at closing that gap. [RFC-100](https://github.com/apache/hudi/issues/14127) adds Lance as a first-class file format inside Hudi, so vectors can sit in the same table as the structured fields, and [RFC-102](https://github.com/apache/hudi/issues/14219) adds native vector similarity search directly on Hudi tables.

[ Lucidchart: Diagram 3 — see assets section ]

Once both of those land, the second store stops being necessary. The CSV still ingests into Hudi, but the descriptions get embedded into a vector column on the same table, and search runs against Hudi directly instead of a downstream copy. The incremental-pull, delete, re-insert loop has nothing left to do at that point. The sync script was only ever a stand-in for a capability Hudi is in the process of growing, and when that capability ships, the cleanest version of this project is the one where you delete the script.

## What this is not

This is not a daemon. Each run processes one batch of changes and then exits, so the demo is really showing a single tick of what would otherwise be a continuous loop. A production version would poll the timeline on an interval and keep the checkpoint in durable storage rather than in memory.

It is also not Spark-free, and that part is not by choice. Hudi 1.0 writes a version 8 table, while hudi-rs and Daft both top out at table version 6, which leaves PySpark as the only option for every write. I went looking for a lighter write path and did not find one that handles this table version.

And it is not an in-place update. Because LanceDB has no way to update a vector, the sync deletes the old row and inserts a new one, which leaves a brief window where the changed record has no vector in the index at all. For a single-writer proof of concept that window does not matter, but it is the kind of thing that would need real handling under concurrent writes.

The goal was a working proof of concept: keep a vector index consistent with a mutable Hudi table, without nightly rebuilds and without full table scans. That part works. The full code is on [GitHub](https://github.com/Pavan-249/lance-hudi).

---

# Assets to generate

Everything below produces the images referenced in the post. Code snippets go through [carbon.now.sh](https://carbon.now.sh). Diagrams go through Lucidchart.

---

## Snippet A: incremental query (carbon.now.sh)

**Theme:** Night Owl | **Language:** Python | **Line numbers:** off | **Window chrome:** on | **Font:** JetBrains Mono 14px | **Padding:** 32px

```python
# Pull only the records written after the last checkpoint.
# One Hudi primitive. No diff, no change table, no full scan.
incr = (
    spark.read.format("hudi")
    .option("hoodie.datasource.query.type", "incremental")
    .option("hoodie.datasource.read.begin.instanttime", checkpoint)
    .load(HUDI_PATH)
    .select("wine_id", "description", "_hoodie_commit_time")
    .toPandas()
)
```

---

## Snippet B: the resync (carbon.now.sh)

**Same settings as Snippet A.**

```python
# Re-embed only the changed descriptions.
vecs = model.encode(incr["description"].tolist(), convert_to_numpy=True)

# LanceDB has no in-place update: delete the stale row, insert the fresh one.
tbl.delete(f"wine_id = {wine_id}")
tbl.add(pa.table({
    "wine_id":     pa.array(incr["wine_id"].tolist(),     type=pa.int64()),
    "description": pa.array(incr["description"].tolist(), type=pa.string()),
    "vector":      pa.array(vecs.tolist(),                type=pa.list_(pa.float32(), 384)),
}))
```

---

## Snippet C: demo terminal output (carbon.now.sh)

**Theme:** One Dark | **Language:** plaintext | **Line numbers:** off | **Window chrome:** on (macOS style) | **Padding:** 32px

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

---

## Diagram 1: two-store architecture (Lucidchart)

**Style reference:** the Lance vs Iceberg side-by-side diagram from lancedb.com/blog. Two panels with dashed outer borders, inner nested boxes, clean sans-serif labels.

**Layout:** two panels side by side, equal width, separated by a vertical gap containing the sync arrow.

**Left panel** — outer dashed border, title "Apache Hudi" (bold, top-left), accent color #FF6B35 (Hudi orange).
Inside, three nested boxes stacked vertically:
1. Box labeled ".hoodie/ Timeline" — contains three small pill shapes in a row: `20260601...` `20260602...` `20260602...` (representing commit timestamps)
2. Box labeled "Parquet Data Files" — three small overlapping file icons with "country / variety / points / description" as a caption underneath
3. Box labeled "Metadata" — single line: "hoodie.properties, schema"

**Right panel** — outer dashed border, title "LanceDB" (bold, top-left), accent color #1A6FFF (LanceDB blue).
Inside, three nested boxes stacked vertically:
1. Box labeled "Fragment Files (.lance)" — three small overlapping file icons with "wine_id / vector[384] / description" caption
2. Box labeled "IVF-PQ Vector Index" — a small grid representing the ANN index structure
3. Box labeled "Metadata JSON" — single line: "schema, index config"

**Center connector:** a bold horizontal arrow pointing right from the Hudi panel to the LanceDB panel.
Label on arrow (centered, bold): `03_demo_sync.py`
Below arrow label, three stacked step lines in smaller text:
```
1. incremental pull
2. re-embed changed rows
3. delete + insert
```

**Bottom caption** (centered, below both panels): "Hudi is the source of truth. LanceDB is the derived index."

---

## Diagram 2: Hudi commit timeline (Lucidchart)

**Purpose:** visualize how the incremental query primitive works. Appears under the "Why Hudi" section.

**Layout:** horizontal timeline bar across the full width of the diagram.

**Timeline bar:** a thick horizontal line labeled "Hudi Commit Timeline" on the left end.

**Commits:** five circular markers on the timeline, left to right.
- C1, C2: filled grey circles (old commits, already processed)
- C3: filled orange circle labeled "checkpoint" with a small flag icon below it
- C4, C5: filled blue circles (new commits, not yet processed)

**Incremental query window:** a light blue shaded rectangle spanning from C3 to C5, with a bracket above it labeled "incremental query window" in blue.

**Records box:** below C4 and C5, draw two downward arrows pointing into a box labeled "Changed records returned" with two rows inside: `wine_0 (description updated)` and `wine_47 (price updated)`.

**Exclusion note:** below C1 and C2, a greyed-out note: "C1, C2 already processed — not re-embedded"

**Caption** (below everything): "Cost scales with the size of the change, not the size of the table."

---

## Diagram 3: future state, RFC-100 + RFC-102 (Lucidchart)

**Purpose:** show that the two-store architecture collapses into one once the RFCs land.

**Layout:** one large single box, centered. Above it, a smaller "before" reference.

**Before reference** (top, small, faded): a miniaturized version of Diagram 1 — two small boxes (Hudi + LanceDB) connected by an arrow, with an "X" over the arrow labeled "sync script". A large downward-pointing arrow labeled "RFC-100 + RFC-102" transitions from this into the main box below.

**Main box** — outer solid border, title "Apache Hudi" (bold, large), accent color #FF6B35.
Inside, four horizontally full-width rows:

| Row | Label | Color |
|-----|-------|-------|
| 1 | Lance File Format (RFC-100) | blue accent, highlighted |
| 2 | Native Vector Search (RFC-102) | blue accent, highlighted |
| 3 | Parquet / ORC (existing formats) | grey, dimmed |
| 4 | Unified Table: structured fields + vector column | white, bold |

Inside Row 4, two sub-columns: "country / variety / points / price / description" on the left, "vector [384-dim]" on the right, with a small separator line between them.

**Caption** (below box): "No sync script. No second store. Vectors live inside the table."

**RFC badges:** two small pill badges to the right of the box title: `RFC-100` and `RFC-102`, each linked to the GitHub issue.
