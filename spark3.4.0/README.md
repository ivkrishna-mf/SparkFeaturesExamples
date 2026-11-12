# Apache Spark 3.4.0 — What's New, with Examples

**Released:** April 2023  
**Official release notes:** https://spark.apache.org/releases/spark-release-3-4-0.html

Spark 3.4 introduces **Spark Connect** — a thin client/server protocol that decouples applications from the
driver — plus a `TIMESTAMP_NTZ` type, column `DEFAULT` values, lateral column aliases, `UNPIVOT`/`melt`,
an `OFFSET` clause, and distributed PyTorch training via `TorchDistributor`.

## Platform & compatibility

- Java 8/11/17, Scala 2.12/2.13, Python 3.7+.
- Spark Connect server ships as a separate package (`spark-connect_<scala>`).

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | Spark Connect (Python client) | Architecture | ✅ | — | — |
| 2 | TIMESTAMP_NTZ type | SQL | ✅ | ✅ | — |
| 3 | Column DEFAULT values | SQL | ✅ | ✅ | ✅ |
| 4 | Lateral column alias | SQL | ✅ | ✅ | ✅ |
| 5 | UNPIVOT / DataFrame.unpivot & melt | SQL / API | ✅ | ✅ | ✅ |
| 6 | OFFSET clause & DataFrame.offset | SQL / API | ✅ | — | ✅ |
| 7 | New array functions & Dataset.to(schema) | SQL / API | ✅ | ✅ | — |
| 8 | TorchDistributor | ML | — | — | — |
| 9 | Protobuf support | Connectors | — | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. Spark Connect (Python client)

A gRPC/Arrow protocol lets a lightweight client build DataFrame plans and send them to a remote Spark Connect server. Start the server with `./sbin/start-connect-server.sh` and connect with `SparkSession.builder.remote("sc://host:15002")`.

```bash
./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
```

### 2. TIMESTAMP_NTZ type

Timestamp *without* time zone (wall-clock time), separate from the session-zone-adjusted `TIMESTAMP` (now also called `TIMESTAMP_LTZ`).

### 3. Column DEFAULT values

`CREATE TABLE t (status STRING DEFAULT 'active')`; omitted columns or the `DEFAULT` keyword in `INSERT` use the declared default.

### 4. Lateral column alias

Reuse an alias defined earlier in the same `SELECT` list: `SELECT price * 0.18 AS tax, price + tax AS total`.

### 5. UNPIVOT / DataFrame.unpivot & melt

Turn columns into rows in SQL (`UNPIVOT`) or the DataFrame API (`unpivot`, alias `melt`).

### 6. OFFSET clause & DataFrame.offset

`LIMIT n OFFSET m` and `df.offset(m)` for skipping rows.

### 7. New array functions & Dataset.to(schema)

`array_append`, `array_insert`, `array_compact`; `df.to(schema)` reorders and up-casts columns to match a target schema; `withColumnsRenamed` for bulk renames.

### 8. TorchDistributor

`pyspark.ml.torch.distributor.TorchDistributor` launches distributed PyTorch training on a Spark cluster.

### 9. Protobuf support

`from_protobuf` / `to_protobuf` functions in the `spark-protobuf` module.

## Behaviour changes to watch when upgrading

- `TIMESTAMP` keeps local-time-zone semantics; set `spark.sql.timestampType=TIMESTAMP_NTZ` to change the default.
- Many errors now carry error classes and SQLSTATEs (e.g. `[DIVIDE_BY_ZERO]`).

## Folder layout

```
spark3.4.0/
├── README.md
├── pyspark_examples/
│   └── spark_3_4_0_features.py
├── scala_examples/
│   └── Spark340Features.scala
└── java_examples/
    └── Spark340Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==3.4.0" pandas pyarrow
python pyspark_examples/spark_3_4_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark340Features.scala
scala> Spark340Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=3.4.0 -Dexamples.dir=../spark3.4.0/scala_examples package
spark-submit --class Spark340Features target/scala-2.12/spark-release-examples_2.12-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=3.4.0 -Dscala.binary.version=2.12 -Djava.release=17 -Dexamples.dir=../spark3.4.0/java_examples
spark-submit --class Spark340Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
