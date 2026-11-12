# Apache Spark 3.3.0 — What's New, with Examples

**Released:** June 2022  
**Official release notes:** https://spark.apache.org/releases/spark-release-3-3-0.html

Spark 3.3 adds **runtime Bloom filter joins**, a family of error-tolerant `try_*` functions for ANSI mode,
`Trigger.AvailableNow` for incremental batch-style streaming, Arrow-native `mapInArrow`, more DSv2 pushdown,
and Java 17 support.

## Platform & compatibility

- Adds Java 17 support (Java 8/11/17).
- Scala 2.12 (default) and 2.13; Python 3.7+.
- Kubernetes: pluggable batch schedulers (Volcano, YuniKorn).

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Runtime Bloom filter join | SQL engine | ✅ | ✅ | — |
| 2 | try_* functions | SQL | ✅ | ✅ | ✅ |
| 3 | ILIKE and timestampadd/timestampdiff | SQL | ✅ | ✅ | ✅ |
| 4 | Trigger.AvailableNow | Structured Streaming | ✅ | ✅ | ✅ |
| 5 | mapInArrow | PySpark | ✅ | — | — |
| 6 | DSv2 aggregate pushdown | Connectors | — | — | — |
| 7 | Vectorized reader for nested Parquet | Connectors | — | — | — |
| 8 | Python UDF profiler | PySpark | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Runtime Bloom filter join

For selective joins Spark builds a Bloom filter from the small side and pushes it into the big side's scan at runtime. Off by default in 3.3: `spark.sql.optimizer.runtime.bloomFilter.enabled=true`.

### 2. try_* functions

`try_add`, `try_subtract`, `try_multiply`, `try_divide`, `try_sum`, `try_avg`, `try_element_at` return NULL instead of raising errors — ideal companions to ANSI mode.

### 3. ILIKE and timestampadd/timestampdiff

Case-insensitive `ILIKE` (plus `LIKE ANY/ALL` variants) and `timestampadd` / `timestampdiff` for unit-based datetime arithmetic.

### 4. Trigger.AvailableNow

Processes everything available at start-up in multiple micro-batches (respecting rate limits), then stops — the scalable replacement for `Trigger.Once`.

### 5. mapInArrow

Like `mapInPandas` but hands your function `pyarrow.RecordBatch` objects, skipping pandas conversion.

### 6. DSv2 aggregate pushdown

Aggregates (`MIN/MAX/COUNT/SUM/AVG` with GROUP BY) can be pushed down to JDBC and other DSv2 sources (`pushDownAggregate` option).

### 7. Vectorized reader for nested Parquet

`spark.sql.parquet.enableNestedColumnVectorizedReader` speeds up reading structs, arrays and maps.

### 8. Python UDF profiler

With `spark.python.profile=true`, `sc.show_profiles()` reports cProfile output for Python/pandas UDFs.

## Behaviour changes to watch when upgrading

- `pyspark.pandas` DataFrame `.to_pandas_on_spark()` renamed to `.pandas_api()`.
- Trigger.Once is effectively superseded by Trigger.AvailableNow.

## Folder layout

```
spark3.3.0/
├── README.md
├── pyspark_examples/
│   └── spark_3_3_0_features.py
├── scala_examples/
│   └── Spark330Features.scala
└── java_examples/
    └── Spark330Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==3.3.0" pandas pyarrow
python pyspark_examples/spark_3_3_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark330Features.scala
scala> Spark330Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=3.3.0 -Dexamples.dir=../spark3.3.0/scala_examples package
spark-submit --class Spark330Features target/scala-2.12/spark-release-examples_2.12-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=3.3.0 -Dscala.binary.version=2.12 -Djava.release=17 -Dexamples.dir=../spark3.3.0/java_examples
spark-submit --class Spark330Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
