# Apache Spark 3.1 (first GA build: 3.1.1) — What's New, with Examples

**Released:** March 2021 (3.1.1 — the 3.1.0 tag was never published as a release)  
**Official release notes:** https://spark.apache.org/releases/spark-release-3-1-1.html

Spark 3.1 is an incremental release focused on **SQL/ANSI compliance, Python usability and streaming**.
It unifies the `CREATE TABLE` syntax, adds CHAR/VARCHAR semantics, makes nested-struct editing practical,
lets streams read from and write to catalog tables, and declares Spark-on-Kubernetes generally available.

## Platform & compatibility

- Scala 2.12, Java 8/11, Python 3.6+ (3.6 deprecated).
- Spark on Kubernetes is GA.
- Pre-built with Hadoop 3.2 by default.

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | unionByName with allowMissingColumns | DataFrame API | ✅ | ✅ | ✅ |
| 2 | Nested struct editing: withField / dropFields | DataFrame API | ✅ | ✅ | ✅ |
| 3 | New functions | SQL | ✅ | ✅ | ✅ |
| 4 | Streaming to/from catalog tables | Structured Streaming | ✅ | ✅ | — |
| 5 | More stream-stream joins | Structured Streaming | — | — | — |
| 6 | Unified CREATE TABLE + CHAR/VARCHAR | SQL | ✅ | — | — |
| 7 | Shuffle hash join improvements | SQL engine | — | — | — |
| 8 | Kubernetes GA + graceful decommissioning | Deployment | — | — | — |
| 9 | Python type stubs & docs | PySpark | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. unionByName with allowMissingColumns

Union two DataFrames whose schemas only partially overlap; missing columns are filled with NULL.

```python
jan.unionByName(feb, allowMissingColumns=True)
```

### 2. Nested struct editing: withField / dropFields

`Column.withField("a.b", value)` adds or replaces a (nested) struct field; `dropFields` removes them — no more rebuilding structs by hand.

### 3. New functions

`nth_value` window function, `regexp_extract_all`, `timestamp_seconds`/`timestamp_millis`/`timestamp_micros`, `current_timezone`, and `percentile_approx` exposed in the Scala/Python function APIs.

### 4. Streaming to/from catalog tables

`DataStreamWriter.toTable(name)` writes a stream into a table and `DataStreamReader.table(name)` reads one.

### 5. More stream-stream joins

Full outer and left semi joins are supported between two streams (with watermarks).

### 6. Unified CREATE TABLE + CHAR/VARCHAR

`CREATE TABLE` without `USING` / `STORED AS` follows one grammar; CHAR(n) pads and VARCHAR(n) enforces its length limit on write.

### 7. Shuffle hash join improvements

Shuffled hash join supports all join types (including full outer) and code generation; subexpression elimination extended to more operators.

### 8. Kubernetes GA + graceful decommissioning

K8s becomes production-ready; executors can be decommissioned gracefully, migrating shuffle and cached blocks before shutdown (`spark.decommission.enabled`).

### 9. Python type stubs & docs

PySpark ships inline type hints (from pyspark-stubs) and a redesigned user guide / API reference.

## Behaviour changes to watch when upgrading

- CHAR(n) values are padded and VARCHAR(n) length is checked — inserts exceeding the length now fail.
- `CREATE TABLE` without `USING` follows `spark.sql.legacy.createHiveTableByDefault` (default true in 3.1).
- Python 3.6 deprecated; 3.1.0 artifacts were not published — use 3.1.1 or later.

## Folder layout

```
spark3.1.0/
├── README.md
├── pyspark_examples/
│   └── spark_3_1_features.py
├── scala_examples/
│   └── Spark310Features.scala
└── java_examples/
    └── Spark310Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==3.1.1" pandas pyarrow
python pyspark_examples/spark_3_1_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark310Features.scala
scala> Spark310Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=3.1.1 -Dexamples.dir=../spark3.1.0/scala_examples package
spark-submit --class Spark310Features target/scala-2.12/spark-release-examples_2.12-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=3.1.1 -Dscala.binary.version=2.12 -Djava.release=11 -Dexamples.dir=../spark3.1.0/java_examples
spark-submit --class Spark310Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
