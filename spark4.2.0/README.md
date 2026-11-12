# Apache Spark 4.2.0 — What's New, with Examples

**Released:** July 2026  
**Official release notes:** https://spark.apache.org/releases/spark-release-4-2-0.html

Spark 4.2 expands Spark's types and SQL surface for analytics and AI workloads: native **geospatial**
`GEOMETRY`/`GEOGRAPHY` types with `ST_*` functions, engine-level **Change Data Capture** (`CHANGES` clause and
DataFrame APIs) with **Auto CDC (SCD Type 1)** in Declarative Pipelines, the **NEAREST BY** top-K ranking join with
vector functions, **metric views**, SQL `PATH` resolution, `QUALIFY`, and **Arrow-optimised Python UDFs on by default**.
It is the last "large" semi-annual drop before Spark moves to quarterly feature releases.

## Platform & compatibility

- Builds and runs on Java 25 (Java 17 minimum).
- Scala 2.13.18; Arrow 19; Parquet 1.17.
- Modernised Spark Web UI (dark mode, searchable/zoomable SQL plans).

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Geospatial types | SQL | ✅ | ✅ | ✅ |
| 2 | Change Data Capture (CHANGES clause) | SQL / Connectors | — | — | — |
| 3 | Auto CDC in Declarative Pipelines (SCD Type 1) | Data engineering | — | — | — |
| 4 | NEAREST BY join + vector functions | SQL / AI | ✅ | ✅ | ✅ |
| 5 | QUALIFY clause | SQL | ✅ | ✅ | ✅ |
| 6 | Metric views | SQL / semantic layer | — | — | — |
| 7 | SQL PATH resolution | SQL | ✅ | — | — |
| 8 | Arrow by default for Python UDFs | PySpark | ✅ | — | — |
| 9 | Streaming: named sources/sinks, RTM in PySpark | Streaming | ✅ | — | — |
| 10 | IGNORE NULLS aggregates, max_by/min_by with K, time_bucket | SQL | ✅ | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Geospatial types

`GEOMETRY` and `GEOGRAPHY` types, `ST_*` functions, WKB/WKT and Parquet read/write, and an SRID registry — enabled by default.

```sql
SELECT hex(ST_AsBinary(ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040'))))
```

Tracking: SPARK-51658

### 2. Change Data Capture (CHANGES clause)

One syntax for reading row-level changes from any DSv2 connector that implements the `Changelog` API (batch and streaming), plus DataFrame/PySpark/Connect APIs.

```sql
SELECT * FROM cat.db.orders CHANGES FROM VERSION 10 TO VERSION 20;
SELECT * FROM STREAM cat.db.orders CHANGES FROM VERSION 0;
```

Tracking: SPARK-55668

### 3. Auto CDC in Declarative Pipelines (SCD Type 1)

`dp.create_auto_cdc_flow(...)` applies an ordered change feed as upserts/deletes into a target table. (SCD Type 2 follows in 4.3 — see `spark4.3.0/`.)

Tracking: SPARK-56249

### 4. NEAREST BY join + vector functions

Top-K nearest-neighbour join ranked by a distance or similarity expression, with vector functions such as `vector_cosine_similarity` and `vector_l2_distance`.

```sql
FROM queries q JOIN docs d
  EXACT NEAREST 2 BY SIMILARITY vector_cosine_similarity(q.emb, d.emb)
```

Tracking: SPARK-56395, SPARK-54713

### 5. QUALIFY clause

Filter on window-function results without a subquery.

Tracking: SPARK-31561

### 6. Metric views

Declare governed business metrics once (`CREATE VIEW ... WITH METRICS`, YAML-defined) and query them consistently from SQL, BI and AI tools.

Tracking: SPARK-54119

### 7. SQL PATH resolution

`SET PATH`, qualified built-in/session names and `current_path()` (enable with `spark.sql.path.enabled`).

Tracking: SPARK-54806

### 8. Arrow by default for Python UDFs

Arrow-optimised Python UDFs and Arrow-based PySpark IPC are on by default; plus PyCapsule (Arrow C data interface) support and `DataFrame.zipWithIndex`.

Tracking: SPARK-54555

### 9. Streaming: named sources/sinks, RTM in PySpark

`.name()` gives streaming sources/sinks stable identities so you can add, remove and reorder sources without breaking checkpoints; Real-Time Mode trigger available from Python.

Tracking: SPARK-54909, SPARK-54660

### 10. IGNORE NULLS aggregates, max_by/min_by with K, time_bucket

`array_agg`/`collect_list`/`collect_set` accept IGNORE/RESPECT NULLS; `max_by(x, y, k)` returns the top-k; new `time_bucket` scalar function.

## Behaviour changes to watch when upgrading

- Arrow-optimised Python UDFs are the default — return-type coercion can differ slightly from pickled UDFs.
- Shuffle checksums are order-independent by default to catch inconsistent retries.
- R 3.x support dropped.

## Folder layout

```
spark4.2.0/
├── README.md
├── pyspark_examples/
│   └── spark_4_2_0_features.py
├── scala_examples/
│   └── Spark420Features.scala
└── java_examples/
    └── Spark420Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==4.2.0" pandas pyarrow
python pyspark_examples/spark_4_2_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark420Features.scala
scala> Spark420Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=4.2.0 -Dexamples.dir=../spark4.2.0/scala_examples package
spark-submit --class Spark420Features target/scala-2.13/spark-release-examples_2.13-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=4.2.0 -Dscala.binary.version=2.13 -Djava.release=17 -Dexamples.dir=../spark4.2.0/java_examples
spark-submit --class Spark420Features target/spark-release-examples-0.1.jar
```

**Notes**

- `CHANGES` and Auto CDC need a table format whose DSv2 connector implements Spark's Changelog / row-level APIs (e.g. a recent Delta Lake or Iceberg release built for 4.2); they are shown as commented snippets.

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
