# Spark ETL Pipeline — Architecture, Optimization & Best Practices

A hands-on PySpark project that builds a small but realistic ETL pipeline
(read → transform → filter → write) while demonstrating the Spark concepts
that actually determine performance in production: lazy evaluation, the
DAG/lineage graph, shuffles, predicate pushdown, and CSV vs Parquet.

Every result in this README (row counts, timings, physical plans, file
sizes) was captured from a real run of the code in `src/`, not written by
hand — re-run it yourself with the commands in [Running It](#running-it).

---

## 1. Spark Architecture

```
                    ┌─────────────────────────┐
                    │        DRIVER            │
                    │  - Builds logical plan    │
                    │  - Catalyst optimizer     │
                    │  - Creates DAG of stages  │
                    │  - Schedules tasks        │
                    └────────────┬─────────────┘
                                 │ requests resources
                                 ▼
                    ┌─────────────────────────┐
                    │     CLUSTER MANAGER       │
                    │ (YARN / Kubernetes /      │
                    │  Spark Standalone /       │
                    │  local[*] in this demo)   │
                    └────────────┬─────────────┘
                                 │ allocates
                 ┌───────────────┼───────────────┐
                 ▼               ▼               ▼
          ┌───────────┐   ┌───────────┐   ┌───────────┐
          │ EXECUTOR 1 │   │ EXECUTOR 2 │   │ EXECUTOR N │
          │  tasks +   │   │  tasks +   │   │  tasks +   │
          │  cache     │   │  cache     │   │  cache     │
          └───────────┘   └───────────┘   └───────────┘
```

- **Driver** — runs the `main()` of your Spark application, holds the
  `SparkSession`, converts your DataFrame/SQL calls into a logical plan,
  optimizes it via **Catalyst**, and turns it into a physical plan of
  stages and tasks.
- **Cluster Manager** — negotiates resources for the application. This repo
  runs in **local mode** (`master("local[*]")`), where the Driver and all
  Executors share a single JVM and `local[*]` just means "use every CPU
  core as an executor slot." In production this would instead be YARN,
  Kubernetes, or Spark Standalone, with Executors running as separate JVMs
  on different worker machines.
- **Executors** — the processes that actually run tasks and hold cached
  partitions in memory. Each task processes one partition of data.

**Execution modes**: `client` (Driver runs on the machine that launched the
job, e.g. your laptop or a notebook — used here), `cluster` (Driver runs
inside the cluster, typical for production `spark-submit` jobs), and
`local` (everything — Driver + Executors — in one JVM, used for
development/testing, which is what this repo uses).

In this run:
```
Spark version: 4.2.0
Default parallelism (executor slots): 1
```
(single-core container in this environment — on a real machine `local[*]`
would report one slot per CPU core.)

---

## 2. Lazy Evaluation and the DAG (Lineage Graph)

Spark distinguishes two kinds of operations:

| Type | Examples | Behavior |
|---|---|---|
| **Transformation** | `select`, `filter`, `withColumn`, `groupBy` | Lazy — only recorded in the logical plan, nothing runs |
| **Action** | `count()`, `show()`, `collect()`, `write()` | Eager — triggers actual execution |

Because transformations are lazy, Spark can see the **entire chain** of
operations before running anything and rewrite it for efficiency — this is
the **DAG (lineage graph)**. Concretely, in `src/01_spark_pipeline.py`, the
`.select()`, `.withColumnRenamed()`, `.withColumn()` calls that build
`transformed_df` do not touch data at all; calling `.explain()` on it right
after shows only a **logical/physical plan**, no execution:

```
== Physical Plan ==
* Project (3)
+- * Project (2)
   +- Scan csv (1)
```

Notice Catalyst already **collapsed multiple `.withColumn()` calls into a
single `Project` node** rather than one node per call — that's the
optimizer working on the DAG before any task runs. Execution only actually
starts a few lines later at `raw_df.count()`, the first action.

Benefits of laziness:
- **Whole-pipeline optimization** (column pruning, filter pushdown, join
  reordering) instead of optimizing operation-by-operation.
- **No wasted work** — if a column is selected then later dropped, Spark
  never bothers computing it.
- **Fault tolerance** — if an Executor dies mid-job, Spark recomputes only
  the lost partitions using the lineage graph, instead of failing the
  whole job.

---

## 3. Schema Handling

Two ways to read CSV:
```python
spark.read.option("header", "true").option("inferSchema", "true").csv(path)   # ❌ extra job to sample types
spark.read.option("header", "true").schema(explicit_schema).csv(path)         # ✅ single pass
```
`inferSchema=True` makes Spark scan the file an **extra time** just to
guess column types — doubling I/O on large files. This project always
defines an explicit `StructType` (see `src/01_spark_pipeline.py`), so the
read is one pass and types are guaranteed correct instead of guessed.

---

## 4. Transformations Applied

| Step | Operation | Purpose |
|---|---|---|
| Select & rename | `.select(...)`, `.withColumnRenamed()` | Keep only needed columns; `customer_id` → `cust_id` |
| Cast/parse | `.withColumn("order_date", F.to_date(...))` | String → `DateType` |
| Normalize | `F.initcap`, `F.upper`, `F.trim` | Fix inconsistent casing (`north` / `SOUTH` / `North`) |
| Derive | `order_value = quantity * unit_price` | New computed column |
| Extract | `order_year = year(order_date)` | New column from existing one |
| Null handling | `.na.drop(subset=[...])`, `.na.fill({...})` | Drop rows missing essential numeric/date fields; fill missing `region` with `"Unknown"` instead of dropping |
| Business filter | `.filter(status != "CANCELLED")` | Exclude cancelled orders from revenue analysis |

Result on the 50,000-row synthetic dataset generated by
`src/00_generate_sample_data.py`:
```
Raw row count:     50000
Clean row count:   37778   (nulls dropped + cancelled orders excluded)
Time for both count() actions: 9.06s
```

---

## 5. Wide Transformations & Shuffle

**Narrow transformations** (`select`, `filter`, `withColumn`) map each
output partition from exactly one input partition — no data movement
between Executors is needed.

**Wide transformations** (`groupBy`, `join`, `distinct`, `orderBy`) require
rows with the same key to end up in the same partition, which forces a
**shuffle**: data is written to disk/network and re-read by other tasks.
This is normally the most expensive part of a Spark job.

The `groupBy("region", "category").agg(...)` in this pipeline produces
exactly that in its physical plan — note the two `Exchange` nodes, which
are the shuffle boundaries:

```
AdaptiveSparkPlan
+- Sort
   +- Exchange              <- shuffle (range partition for global ORDER BY)
      +- HashAggregate       <- final merge of partial aggregates
         +- Exchange          <- shuffle (hash partition by region, category)
            +- HashAggregate  <- partial (map-side) aggregation per partition
               +- Project
                  +- Filter
                     +- Scan csv
```

Note Spark does a **partial aggregation before the shuffle** (map-side
combine, similar to a MapReduce combiner) so far less data crosses the
network than if every raw row were shuffled individually — this is an
automatic optimization, not something we coded.

Actual output (top rows by revenue):
```
+-------+-----------+----------+------------------+------------------+
|region |category   |num_orders|total_revenue     |avg_order_value   |
+-------+-----------+----------+------------------+------------------+
|North  |Toys       |2188      |3149140.17        |1439.28           |
|South  |Electronics|2232      |3083322.07        |1381.42           |
|South  |Clothing   |2186      |3029905.33        |1386.05           |
...
```

---

## 6. Predicate Pushdown: CSV vs Parquet

**Predicate pushdown** means the filter condition is evaluated as close to
the data source as possible — ideally the file format itself skips
irrelevant data before Spark ever deserializes a row.

- **Parquet** is columnar and stores per-file/per-row-group statistics
  (min/max), so filters can skip entire blocks of data without reading
  them. When the filtered column is also a **partition column**, Spark
  does something even cheaper: **partition pruning** — skipping whole
  folders. That's exactly what happened when filtering the Parquet output
  (partitioned by `region`) for `region == "North"`:

  ```
  Scan parquet
  PartitionFilters: [isnotnull(region#154), (region#154 = North)]
  ```
  Only the `region=North/` folder is ever touched — the other four
  region folders are never opened.

- **CSV** is row-based, uncompressed-per-column, and has no built-in
  statistics. Spark still reports a `PushedFilters` entry when filtering
  CSV by `status`, but this only means the filter predicate is passed down
  to the row scanner to short-circuit as it parses each row — it still
  requires reading and parsing **every row of the file**, unlike Parquet's
  ability to skip whole blocks/folders based on stored statistics:

  ```
  Scan csv
  PushedFilters: [IsNotNull(status), EqualTo(status,PENDING)]
  ```

**Practical takeaway**: for a dataset that's queried repeatedly with
filters on the same column, converting to Parquet and partitioning by that
column turns an $O(\text{full file scan})$ operation into an
$O(\text{matching partitions only})$ one.

---

## 7. File Format Comparison: CSV vs Parquet

Same cleaned dataset (37,778 rows), written both ways:

| Format | On-disk size | Notes |
|---|---|---|
| CSV (`orders_clean_csv/`) | **2.9 MB** | Row-based, plain text, no compression, no embedded schema |
| Parquet (`orders_clean.parquet/`, partitioned by `region`) | **892 KB** | Columnar, Snappy-compressed, embedded schema, partition-pruned reads |

Parquet came out **~3.3x smaller** here, plus it enables column pruning
(reading only the columns you `select`, not the whole row) and predicate
pushdown, both of which CSV cannot do. CSV remains useful mainly for
interoperability with non-Spark tools or human-readability of small
extracts.

---

## 8. Best Practices Followed in This Repo

- ✅ Explicit schema instead of `inferSchema=True`
- ✅ `show()` / `count()` for inspection instead of `collect()` — `collect()`
  pulls the **entire** result to the Driver's memory, which is fine for a
  handful of rows but can OOM the Driver on large datasets. This pipeline
  never calls `collect()`.
- ✅ Filter early, select only needed columns — lets Catalyst push filters
  and column pruning down to the scan
- ✅ Partition Parquet output by a **low-cardinality** column (`region`, 5
  distinct values) — partitioning by a high-cardinality column like
  `order_id` would create thousands of tiny files and hurt performance
  instead of helping it
- ✅ Reduced `spark.sql.shuffle.partitions` to 8 for this small local
  dataset (default 200 is tuned for cluster-scale data and would create
  200 mostly-empty tiny output files here)

---

## 9. Repository Structure

```
spark-etl-pipeline/
├── README.md
├── requirements.txt
├── data/
│   ├── raw/
│   │   └── orders.csv                 # generated synthetic input
│   └── output/
│       ├── orders_clean_csv/          # cleaned data, CSV
│       ├── orders_clean.parquet/      # cleaned data, Parquet, partitioned by region
│       └── region_category_summary.parquet/
└── src/
    ├── 00_generate_sample_data.py     # creates data/raw/orders.csv
    └── 01_spark_pipeline.py           # full read -> transform -> filter -> write pipeline
```

---

## Running It

```bash
pip install -r requirements.txt

# 1. Generate the raw synthetic dataset (50,000 rows, with nulls & messy casing)
python3 src/00_generate_sample_data.py

# 2. Run the full pipeline: read, transform, filter, aggregate, write CSV + Parquet
python3 src/01_spark_pipeline.py
```

Requires Java 8/11/17/21 on the `PATH` (Spark runs on the JVM). No cluster
needed — everything runs in `local[*]` mode.

---

## Key Insights Summary

1. **Laziness lets the optimizer see the whole pipeline** — Catalyst
   collapsed several chained `withColumn` calls into a single `Project`
   node before any data was touched.
2. **Shuffles (wide transformations) are the expensive part** — `groupBy`
   introduced two `Exchange` nodes in the physical plan; Spark
   automatically pre-aggregates per-partition before shuffling to cut the
   data volume moved across the network.
3. **File format changes what "filter" means physically** — the same
   logical filter compiles to a full-file parse-then-filter on CSV, but to
   folder-level partition pruning on partitioned Parquet.
4. **Parquet wins on both size and query speed** for anything read more
   than once — 892 KB vs 2.9 MB for identical data in this run, plus
   column pruning and pushdown that CSV cannot offer.
5. **Explicit schemas and `show()`/`count()` over `collect()`** are cheap
   habits that avoid, respectively, an extra scanning job and Driver OOMs
   on large result sets.
