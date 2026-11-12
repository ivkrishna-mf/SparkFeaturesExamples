# Apache Spark 3.5.0 — What's New, with Examples

**Released:** September 2023 (3.5 is the long-term-support line of 3.x)  
**Official release notes:** https://spark.apache.org/releases/spark-release-3-5-0.html

Spark 3.5, the last and longest-supported 3.x release, broadens **Spark Connect** (Scala client, more APIs),
adds **Python UDTFs** and Arrow-optimised Python UDFs, typed **parameterized SQL** with named and positional
markers, the `IDENTIFIER` clause, HyperLogLog sketch functions, PySpark testing utilities, and
`dropDuplicatesWithinWatermark` for streaming.

## Platform & compatibility

- Java 8/11/17, Scala 2.12/2.13, Python 3.8+.
- Spark Connect gains a Scala client and broader API coverage.

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Python UDTFs (user-defined table functions) | PySpark | ✅ | — | — |
| 2 | Arrow-optimised Python UDFs | PySpark | ✅ | — | — |
| 3 | Parameterized SQL (named and positional) | SQL | ✅ | ✅ | ✅ |
| 4 | IDENTIFIER clause | SQL | ✅ | ✅ | — |
| 5 | HyperLogLog sketches | SQL | ✅ | ✅ | ✅ |
| 6 | dropDuplicatesWithinWatermark | Structured Streaming | ✅ | ✅ | — |
| 7 | PySpark testing utilities | PySpark | ✅ | — | — |
| 8 | New array functions | SQL | ✅ | — | — |
| 9 | DeepspeedTorchDistributor & RocksDB changelog checkpointing | ML / Streaming | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Python UDTFs (user-defined table functions)

Write a Python class with `eval()` that yields rows; call it from the DataFrame API or register it and use it in SQL `FROM`.

```python
@udtf(returnType="word: string, length: int")
class SplitWords:
    def eval(self, text: str):
        for w in text.split():
            yield w, len(w)
```

### 2. Arrow-optimised Python UDFs

`@udf(..., useArrow=True)` (or `spark.sql.execution.pythonUDF.arrow.enabled`) moves data to Python workers with Arrow instead of pickling — often much faster.

### 3. Parameterized SQL (named and positional)

Bind typed values safely with `:name` or `?` markers instead of string formatting.

```python
spark.sql("SELECT * FROM t WHERE id > :min", args={"min": 5})
```

### 4. IDENTIFIER clause

`IDENTIFIER(:tbl)` turns a parameter into a table, column or function name — parameterized DDL/DML without SQL injection.

### 5. HyperLogLog sketches

`hll_sketch_agg`, `hll_union_agg`, `hll_union`, `hll_sketch_estimate` — mergeable approximate distinct counts you can store and roll up later.

### 6. dropDuplicatesWithinWatermark

Deduplicates events whose duplicates arrive within the watermark delay, even when event timestamps differ.

### 7. PySpark testing utilities

`pyspark.testing.assertDataFrameEqual` and `assertSchemaEqual` with readable diffs.

### 8. New array functions

`array_prepend` joins the 3.4 additions; plus more `try_*` functions.

### 9. DeepspeedTorchDistributor & RocksDB changelog checkpointing

Distributed DeepSpeed training on Spark; RocksDB state store can checkpoint changelogs instead of full snapshots (`spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled`).

## Behaviour changes to watch when upgrading

- In Python, `spark.sql(..., args=...)` now takes Python values (3.4 expected SQL literal strings).
- 3.5 is the 3.x LTS line — the natural stepping stone before moving to 4.x.

## Folder layout

```
spark3.5.0/
├── README.md
├── pyspark_examples/
│   └── spark_3_5_0_features.py
├── scala_examples/
│   └── Spark350Features.scala
└── java_examples/
    └── Spark350Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==3.5.0" pandas pyarrow
python pyspark_examples/spark_3_5_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark350Features.scala
scala> Spark350Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=3.5.0 -Dexamples.dir=../spark3.5.0/scala_examples package
spark-submit --class Spark350Features target/scala-2.12/spark-release-examples_2.12-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=3.5.0 -Dscala.binary.version=2.12 -Djava.release=17 -Dexamples.dir=../spark3.5.0/java_examples
spark-submit --class Spark350Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
