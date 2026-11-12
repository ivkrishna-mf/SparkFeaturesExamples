# Apache Spark 3.2.0 — What's New, with Examples

**Released:** October 2021  
**Official release notes:** https://spark.apache.org/releases/spark-release-3-2-0.html

Spark 3.2 brings **pandas to Spark** (the Koalas project merged in as `pyspark.pandas`), turns
**Adaptive Query Execution on by default**, introduces ANSI interval types, session windows for streaming,
a RocksDB state store, and push-based shuffle.

## Platform & compatibility

- First release with Scala 2.13 artifacts (Scala 2.12 remains the default).
- Java 8/11, Python 3.6+.
- Default Hadoop upgraded to 3.3.

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Pandas API on Spark | PySpark | ✅ | — | — |
| 2 | AQE enabled by default | SQL engine | ✅ | — | ✅ |
| 3 | ANSI interval types | SQL | ✅ | ✅ | ✅ |
| 4 | Session windows | Structured Streaming / SQL | ✅ | ✅ | ✅ |
| 5 | RocksDB state store | Structured Streaming | ✅ | ✅ | — |
| 6 | LATERAL subqueries | SQL | ✅ | — | ✅ |
| 7 | Push-based shuffle | Core | — | — | — |
| 8 | Parquet modular encryption & zstd | Connectors | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Pandas API on Spark

`import pyspark.pandas as ps` gives a pandas-compatible API that executes on Spark — the former Koalas project, now built in.

```python
import pyspark.pandas as ps
psdf = ps.read_csv("s3://bucket/data.csv")
psdf.groupby("city").temp.mean()
```

### 2. AQE enabled by default

`spark.sql.adaptive.enabled` now defaults to `true`, so partition coalescing, join switching and skew handling apply to every query.

### 3. ANSI interval types

`INTERVAL YEAR TO MONTH` and `INTERVAL DAY TO SECOND` are real, comparable, storable types; timestamp subtraction returns a day-time interval.

```sql
SELECT DATE'2021-10-13' + INTERVAL '1-6' YEAR TO MONTH;
```

### 4. Session windows

`session_window(ts, gap)` groups events into activity sessions that close after a period of inactivity.

### 5. RocksDB state store

Stateful streaming queries can keep state in RocksDB off-heap instead of the JVM heap, removing GC pressure for large state.

```sql
spark.sql.streaming.stateStore.providerClass=org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider
```

### 6. LATERAL subqueries

A subquery in `FROM` can reference columns of preceding tables with `LATERAL`.

### 7. Push-based shuffle

Map tasks push and merge shuffle blocks on remote shuffle services (YARN), cutting small random reads. Enable with `spark.shuffle.push.enabled=true` plus the external shuffle service.

### 8. Parquet modular encryption & zstd

Columnar encryption for Parquet (Parquet 1.12) and zstd compression improvements.

## Behaviour changes to watch when upgrading

- AQE is now on by default — explain plans start with `AdaptiveSparkPlan` and partition counts change at runtime.
- Timestamp/date subtraction returns ANSI interval types; `spark.sql.legacy.interval.enabled=true` restores CalendarInterval.
- Koalas users: switch `import databricks.koalas as ks` to `import pyspark.pandas as ps`.

## Folder layout

```
spark3.2.0/
├── README.md
├── pyspark_examples/
│   └── spark_3_2_0_features.py
├── scala_examples/
│   └── Spark320Features.scala
└── java_examples/
    └── Spark320Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==3.2.0" pandas pyarrow
python pyspark_examples/spark_3_2_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark320Features.scala
scala> Spark320Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=3.2.0 -Dexamples.dir=../spark3.2.0/scala_examples package
spark-submit --class Spark320Features target/scala-2.12/spark-release-examples_2.12-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=3.2.0 -Dscala.binary.version=2.12 -Djava.release=11 -Dexamples.dir=../spark3.2.0/java_examples
spark-submit --class Spark320Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
