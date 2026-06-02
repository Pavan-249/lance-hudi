# Handoff: lance-hudi

Context for picking this project up in a new session or with a different model.

## What this project is

A working POC that keeps a LanceDB vector index in sync with a mutable Apache Hudi table. When a record's description changes in Hudi, the sync detects it via an incremental query, deletes the stale embedding from LanceDB, and inserts a fresh one. No full table scans, no index rebuilds. Dataset: 130k Kaggle wine reviews.

## What is done

- `scripts/01_ingest_hudi.py`: reads winemag-data-130k-v2.csv, writes to Hudi COW table via PySpark
- `scripts/02_embed_lance.py`: reads Hudi snapshot, embeds descriptions with all-MiniLM-L6-v2, writes to LanceDB
- `scripts/03_demo_sync.py`: the main demo. Mutates a record in Hudi, runs incremental pull, resyncs LanceDB, verifies with semantic search
- `README.md`: full project README with setup, run order, architecture, why Hudi, what's next, limitations
- `BLOG.md`: blog post draft with all prose + instructions to generate carbon.sh screenshots and Lucidchart diagrams (see the assets section at the bottom)
- Portfolio blog: `/Users/pavankumar_s/Desktop/pavankumar-portfolio/src/content/blog/lance-hudi.md` (has image placeholders to fill in once assets are generated)

## What is pending

Three carbon.sh screenshots and three Lucidchart diagrams. Specs are in `BLOG.md` under "Assets to generate":
- Snippet A: the incremental query code
- Snippet B: the resync delete+insert code
- Snippet C: the terminal demo output
- Diagram 1: two-store architecture (Hudi + LanceDB side by side)
- Diagram 2: Hudi commit timeline (shows checkpoint + incremental window)
- Diagram 3: future state single-box (RFC-100 + RFC-102)

Once images are generated, save them to `/Users/pavankumar_s/Desktop/pavankumar-portfolio/public/images/blog/` and replace the `![...](/images/blog/lance-hudi-*.png)` placeholders in `lance-hudi.md`.

## Tech stack

PySpark 3.5, Hudi 1.0.2 COW (jar: `jars/hudi-spark3.5-bundle_2.12-1.0.2.jar`), LanceDB, sentence-transformers (all-MiniLM-L6-v2), PyArrow, Python 3.11, Rich.

## Key constraints

- Hudi writes require PySpark. hudi-rs and Daft cap at table version 6. This table is version 8 (Hudi 1.0.2).
- LanceDB has no in-place vector update. Sync is delete + insert.
- The sync is per-run, not a daemon. No checkpoint persistence across runs in the current demo.

## Run order

```bash
python scripts/01_ingest_hudi.py   # ~5 min, writes 130k rows to Hudi
python scripts/02_embed_lance.py   # ~10 min, embeds and loads into LanceDB
python scripts/03_demo_sync.py     # ~2 min, runs the full sync demo
```

## Portfolio site

Astro project at `/Users/pavankumar_s/Desktop/pavankumar-portfolio/`. Blog posts are markdown files under `src/content/blog/`. The layout is `src/layouts/BlogPost.astro`. No auto-generated ToC in the layout, the blog post itself has a manual Contents section.

## GitHub

Remote: https://github.com/Pavan-249/lance-hudi  
Committer: pavan-249 / pavancseds@gmail.com

## Writing rules (for any prose edits)

- No em-dashes. Use commas, colons, or shorter sentences.
- No AI filler: "delve", "it's worth noting", "in conclusion", "at its core".
- Varied sentence length. No staccato fragment chains. Default to flowing compound sentences.
- First person is fine.
- No bullet points in prose sections.
