# Apache Spark 4.3.0 — What's New, with Examples

**Released:** October 2026 (first release on the new quarterly feature-release train)  
**Official release notes:** https://spark.apache.org/releases/spark-release-4-3-0.html

Spark 4.3 brings **stateful processing to Real-Time Mode** (aggregations, `dropDuplicates`,
`transformWithState` at ~100 ms end-to-end), **SCD Type 2 Auto CDC** in Declarative Pipelines, and faster
small-data iteration (shuffle-free single-task execution, Arrow-native caching). SQL adds `ASOF JOIN`,
`JSON_TABLE`, ANSI `UNNEST`, VARIANT editing functions, and a preview of **nanosecond timestamps**. Spark Connect
gains REST endpoints for sessions/operations and a more complete JDBC driver.

## Platform & compatibility

- Python 3.10 dropped — Python 3.11+ required; NumPy 1.23.2+.
- Java 17+ (Java 26 cleaner support added); Docker images default to Ubuntu 26.04 + Python 3.14.
- Feature releases are now quarterly with 6 months of maintenance each; the 4.x LTS will be 4.5.

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Stateful Real-Time Mode | Streaming | — | ✅ | — |
| 2 | Auto CDC SCD Type 2 | Data engineering | ✅ | — | — |
| 3 | ASOF JOIN (preview) | SQL | ✅ | ✅ | ✅ |
| 4 | JSON_TABLE | SQL | ✅ | ✅ | ✅ |
| 5 | ANSI UNNEST [WITH ORDINALITY] | SQL | ✅ | ✅ | ✅ |
| 6 | VARIANT editing functions | SQL | ✅ | ✅ | — |
| 7 | Nanosecond timestamps (preview) | SQL | ✅ | — | — |
| 8 | Faster small-data queries | Performance | ✅ | — | — |
| 9 | Python UDF transpilation (experimental) | PySpark | ✅ | — | — |
| 10 | New functions & DataFrame.zip | SQL / API | ✅ | — | ✅ |
| 11 | Spark Connect REST API, operation IDs, JDBC complex types | Connect | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Stateful Real-Time Mode

Streaming aggregation, `dropDuplicates` and Scala/Java `transformWithState` run in Real-Time Mode, built on concurrent stage scheduling and pipelined shuffles (`PipelinedShuffleDependency`). RTM is no longer `@Experimental`.

Tracking: SPARK-58508, SPARK-58635, SPARK-57228

### 2. Auto CDC SCD Type 2

`stored_as_scd_type=2` / `STORED AS SCD TYPE 2` keeps each key's history as versioned rows with `__START_AT` / `__END_AT`. See `pyspark_examples/pipelines/`.

Tracking: SPARK-58321, SPARK-58247, SPARK-58261

### 3. ASOF JOIN (preview)

Point-in-time join: match each row with the latest row on the other side at or before its timestamp. Off by default — `SET spark.sql.join.asofJoin.enabled = true`.

```sql
FROM trades t
LEFT ASOF JOIN quotes q
  MATCH_CONDITION (t.ts >= q.ts)
  ON t.symbol = q.symbol
```

Tracking: SPARK-58092

### 4. JSON_TABLE

SQL-standard table function that shreds a JSON array into typed rows (flat form; `NESTED PATH` not yet supported).

Tracking: SPARK-58366

### 5. ANSI UNNEST [WITH ORDINALITY]

Expand one or more arrays in `FROM`; multiple arrays expand in parallel with NULL padding; `WITH ORDINALITY` adds a 1-based position.

Tracking: SPARK-58419

### 6. VARIANT editing functions

`variant_set`, `variant_insert`, `variant_delete`, `variant_array_append`, `variant_strip_nulls` (+ `try_` variants) modify semi-structured values in place.

Tracking: SPARK-57804, SPARK-57641, SPARK-57417

### 7. Nanosecond timestamps (preview)

`TIMESTAMP_NTZ(p)` / `TIMESTAMP_LTZ(p)` with p = 7..9 behind `spark.sql.timestampNanosTypes.enabled`; `TIME(p)` extended to nanoseconds.

Tracking: SPARK-56876

### 8. Faster small-data queries

Opt-in shuffle-free single-task execution (`spark.sql.optimizer.singleTaskExecution.enabled`) and an Arrow-native cache serializer (`ArrowCachedBatchSerializer`).

Tracking: SPARK-57851, SPARK-57268

### 9. Python UDF transpilation (experimental)

With `spark.sql.experimental.optimizer.transpilePyUDFs`, simple Python UDFs compile to Catalyst expressions and skip the Python worker.

Tracking: SPARK-55206

### 10. New functions & DataFrame.zip

`jaro_winkler_similarity`, `counter_diff`, `collect_union`, `to_base32`/`from_base32`, `LOCALTIME`; `DataFrame.zip` merges column projections of the same DataFrame.

### 11. Spark Connect REST API, operation IDs, JDBC complex types

REST endpoints to list/inspect sessions and operations, end-to-end operation IDs, `with` statement support for sessions, and ARRAY/MAP/STRUCT in the Connect JDBC driver.

## Behaviour changes to watch when upgrading

- Python 3.10 is no longer supported.
- `foreachBatch` on Spark Connect runs in a cloned session.
- `spark.rdd.compress` and `spark.ui.contentSecurityPolicy.enabled` now default to true.
- Check that your table-format connectors (Delta, Iceberg, ...) have a build for 4.3 before upgrading.

## Folder layout

```
spark4.3.0/
├── README.md
├── pyspark_examples/
│   ├── spark_4_3_0_features.py
│   └── pipelines/  (Auto CDC SCD Type 2: spark-pipeline.yml + transformations/)
├── scala_examples/
│   └── Spark430Features.scala
└── java_examples/
    └── Spark430Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==4.3.0" pandas pyarrow
python pyspark_examples/spark_4_3_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark430Features.scala
scala> Spark430Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=4.3.0 -Dexamples.dir=../spark4.3.0/scala_examples package
spark-submit --class Spark430Features target/scala-2.13/spark-release-examples_2.13-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=4.3.0 -Dscala.binary.version=2.13 -Djava.release=17 -Dexamples.dir=../spark4.3.0/java_examples
spark-submit --class Spark430Features target/spark-release-examples-0.1.jar
```

**Notes**

- The Auto CDC pipeline reads `raw.customer_cdc_feed` and needs an Iceberg-style catalog for the SCD2 target.
- Stateful Real-Time Mode needs Kafka; `realTimeStatefulSketch` in the Scala file is not run by `main`.

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
