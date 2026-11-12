# Apache Spark 4.1.0 — What's New, with Examples

**Released:** December 2025  
**Official release notes:** https://spark.apache.org/releases/spark-release-4.1.0.html

Spark 4.1 focuses on higher-level data engineering, lower-latency streaming, faster PySpark and a richer SQL
surface. Headline additions: **Spark Declarative Pipelines (SDP)**, **Structured Streaming Real-Time Mode**
(Scala, stateless), Arrow-native Python UDFs/UDTFs, Python Data Source filter pushdown, **SQL scripting GA**,
**VARIANT GA** with shredding and `:` path syntax, **recursive CTEs**, new sketch functions, and a JDBC driver
for Spark Connect.

## Platform & compatibility

- Java 17/21, Scala 2.13.17.
- Python 3.10+ minimum (3.14 supported); PyArrow 15+.
- PySpark maps `BinaryType` to Python `bytes` by default.

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Spark Declarative Pipelines (SDP) | Data engineering | ✅ | — | — |
| 2 | Real-Time Mode for Structured Streaming | Streaming | — | ✅ | — |
| 3 | SQL scripting GA | SQL | ✅ | ✅ | ✅ |
| 4 | VARIANT GA + shredding + `:` syntax | SQL | ✅ | ✅ | ✅ |
| 5 | Recursive CTEs | SQL | ✅ | ✅ | ✅ |
| 6 | Arrow-native Python UDF / UDTF | PySpark | ✅ | — | — |
| 7 | Python Data Source filter pushdown | PySpark | ✅ | — | — |
| 8 | Sketch & approximate functions | SQL | ✅ | — | ✅ |
| 9 | Spark Connect JDBC driver & ML on Connect GA | Connect | — | — | — |
| 10 | Stored procedures API & DSv2 join pushdown | Connectors | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Spark Declarative Pipelines (SDP)

Declare materialized views, streaming tables and flows in Python or SQL; `spark-pipelines run` builds the dependency graph and executes it with checkpoints and retries. See `pyspark_examples/pipelines/`.

```python
from pyspark import pipelines as dp

@dp.materialized_view
def customer_revenue():
    return spark.table("raw_orders").groupBy("cust_id").sum("amount")
```

Tracking: SPARK-51727

### 2. Real-Time Mode for Structured Streaming

A new trigger that runs long-lived batches and passes records between stages as they arrive — sub-second, down to single-digit-millisecond latency for stateless queries (Scala in 4.1, update mode).

Tracking: SPARK-53736

### 3. SQL scripting GA

Enabled by default, with `CONTINUE HANDLER` error handling and multi-variable `DECLARE`.

Tracking: SPARK-54499

### 4. VARIANT GA + shredding + `:` syntax

VARIANT is enabled by default; Parquet writes can shred variant fields for faster reads; `v:path` extracts fields concisely.

```sql
SELECT v:user.id::int FROM events
```

Tracking: SPARK-54454

### 5. Recursive CTEs

`WITH RECURSIVE` for hierarchies and graph walks.

Tracking: SPARK-24497

### 6. Arrow-native Python UDF / UDTF

`@arrow_udf` / `@arrow_udtf` hand your function PyArrow arrays directly — no pandas conversion.

Tracking: SPARK-52214, SPARK-52979

### 7. Python Data Source filter pushdown

Implement `pushFilters()` on a Python `DataSourceReader` to receive predicates and skip data at the source.

Tracking: SPARK-51271

### 8. Sketch & approximate functions

`approx_top_k`, KLL quantile sketches, Theta sketches and `bitmap_and_agg`, among 77 new built-in functions.

Tracking: SPARK-52515, SPARK-54199, SPARK-52407

### 9. Spark Connect JDBC driver & ML on Connect GA

BI tools and JVM apps can talk to Spark Connect through JDBC; Spark ML over Connect is GA for Python.

Tracking: SPARK-53484

### 10. Stored procedures API & DSv2 join pushdown

Catalogs can expose stored procedures (`CALL`); JDBC connectors (Postgres, Oracle, MySQL, SQL Server) can push joins down.

Tracking: SPARK-44167, SPARK-52187

## Behaviour changes to watch when upgrading

- PySpark `BinaryType` → `bytes` (was sometimes `bytearray`); `spark.sql.execution.pyspark.binaryAsBytes=false` reverts.
- Python 3.9 no longer supported.
- ANSI mode on by default in the pandas API on Spark.

## Folder layout

```
spark4.1.0/
├── README.md
├── pyspark_examples/
│   ├── spark_4_1_0_features.py
│   └── pipelines/  (spark-pipeline.yml + transformations/)
├── scala_examples/
│   └── Spark410Features.scala
└── java_examples/
    └── Spark410Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==4.1.0" pandas pyarrow
python pyspark_examples/spark_4_1_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark410Features.scala
scala> Spark410Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=4.1.0 -Dexamples.dir=../spark4.1.0/scala_examples package
spark-submit --class Spark410Features target/scala-2.13/spark-release-examples_2.13-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=4.1.0 -Dscala.binary.version=2.13 -Djava.release=17 -Dexamples.dir=../spark4.1.0/java_examples
spark-submit --class Spark410Features target/spark-release-examples-0.1.jar
```

**Notes**

- Declarative Pipelines: `pip install "pyspark[pipelines]==4.1.0"`, then `cd pyspark_examples/pipelines && spark-pipelines run`.
- Real-Time Mode needs a Kafka source/sink; the Scala method `realTimeModeSketch` shows the shape but is not run by `main`.

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
