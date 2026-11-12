# Apache Spark 3.0.0 — What's New, with Examples

**Released:** June 2020  
**Official release notes:** https://spark.apache.org/releases/spark-release-3-0-0.html

Spark 3.0 was the first major release since 2.0 and is mostly a **performance and SQL-compliance** release.
The query engine learned to re-optimise itself at runtime (Adaptive Query Execution), skip partitions it
learns are irrelevant during a join (Dynamic Partition Pruning), and gained an optional ANSI SQL mode.
PySpark got a redesigned, type-hint-driven Pandas UDF API.

## Platform & compatibility

- Scala 2.12 only (Scala 2.11 dropped).
- Java 8 and Java 11.
- Python 3.6+ (Python 2 and Python 3.5 deprecated).
- Pre-built against Hadoop 2.7 and Hadoop 3.2; Hive 2.3 is the default built-in Hive.
- Calendar switched from hybrid Julian+Gregorian to the Proleptic Gregorian calendar (java.time API).

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Adaptive Query Execution (AQE) | SQL engine | ✅ | ✅ | ✅ |
| 2 | Dynamic Partition Pruning (DPP) | SQL engine | — | ✅ | — |
| 3 | New join hints | SQL | ✅ | ✅ | ✅ |
| 4 | Pandas UDFs with Python type hints | PySpark | ✅ | — | — |
| 5 | ANSI SQL mode | SQL | ✅ | — | ✅ |
| 6 | Formatted EXPLAIN | SQL | ✅ | ✅ | ✅ |
| 7 | New built-in functions & Scala higher-order functions | SQL / API | ✅ | ✅ | ✅ |
| 8 | Binary file data source | Connectors | ✅ | — | — |
| 9 | Structured Streaming UI & GPU-aware scheduling | Ops | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Adaptive Query Execution (AQE)

Re-plans a query between stages using real shuffle statistics: coalesces small shuffle partitions, switches sort-merge joins to broadcast joins, and splits skewed partitions. Ships in 3.0 but is **off by default**.

```sql
SET spark.sql.adaptive.enabled = true;
SET spark.sql.adaptive.coalescePartitions.enabled = true;
SET spark.sql.adaptive.skewJoin.enabled = true;
```

### 2. Dynamic Partition Pruning (DPP)

When a partitioned fact table is joined to a filtered dimension, Spark pushes the dimension filter into the fact scan at runtime so whole partitions are never read. On by default (`spark.sql.optimizer.dynamicPartitionPruning.enabled`).

### 3. New join hints

Alongside `BROADCAST`, 3.0 adds `MERGE` (sort-merge), `SHUFFLE_HASH` and `SHUFFLE_REPLICATE_NL` (cartesian-style nested loop) hints.

```sql
SELECT /*+ SHUFFLE_HASH(c) */ * FROM orders o JOIN customers c ON o.cust_id = c.cust_id
```

### 4. Pandas UDFs with Python type hints

Pandas UDF flavour is now inferred from type hints (`pd.Series -> pd.Series`, `Iterator[pd.Series]`, ...). New pandas function APIs: `mapInPandas` and co-grouped `cogroup(...).applyInPandas(...)`.

```python
@pandas_udf("double")
def with_tax(amount: pd.Series) -> pd.Series:
    return amount * 1.2
```

### 5. ANSI SQL mode

`spark.sql.ansi.enabled=true` makes Spark raise errors on integer overflow and invalid casts instead of silently returning wrapped values or NULL; also enforces ANSI store-assignment rules on insert.

### 6. Formatted EXPLAIN

`EXPLAIN FORMATTED` / `df.explain("formatted")` prints a compact operator tree followed by per-node details, far easier to read than the old extended plan.

### 7. New built-in functions & Scala higher-order functions

Aggregates such as `count_if`, `max_by`, `min_by`, `bool_and`, `bool_or`; constructors `make_date`, `make_timestamp`, `make_interval`. The Scala DataFrame API gains `transform`, `filter`, `exists`, `forall` and `aggregate` for arrays.

### 8. Binary file data source

`spark.read.format("binaryFile")` loads whole files (images, PDFs, models) as rows with path, modificationTime, length and content columns.

### 9. Structured Streaming UI & GPU-aware scheduling

A new *Structured Streaming* tab in the Spark UI shows input rate, processing rate, batch duration and operation duration per query. Accelerator-aware scheduling lets tasks request GPUs (`spark.task.resource.gpu.amount`).

## Behaviour changes to watch when upgrading

- Datetime parsing/formatting moved to `java.time`; some patterns behave differently — `spark.sql.legacy.timeParserPolicy=LEGACY` restores the old behaviour.
- Dates/timestamps before 1582-10-15 may read differently from files written by Spark 2.x (see the `spark.sql.legacy.parquet.datetimeRebaseModeInRead` family of configs).
- Store assignment on `INSERT` follows ANSI rules by default: unsafe casts such as string → int now fail.
- `spark.sql.legacy.*` flags exist for most behaviour changes; check the SQL migration guide before upgrading.

## Folder layout

```
spark3.0.0/
├── README.md
├── pyspark_examples/
│   └── spark_3_0_0_features.py
├── scala_examples/
│   └── Spark300Features.scala
└── java_examples/
    └── Spark300Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==3.0.0" pandas pyarrow
python pyspark_examples/spark_3_0_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark300Features.scala
scala> Spark300Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=3.0.0 -Dexamples.dir=../spark3.0.0/scala_examples package
spark-submit --class Spark300Features target/scala-2.12/spark-release-examples_2.12-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=3.0.0 -Dscala.binary.version=2.12 -Djava.release=11 -Dexamples.dir=../spark3.0.0/java_examples
spark-submit --class Spark300Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
