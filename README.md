# Apache Spark Release Features — 3.0 → 4.3

A hands-on reference to what each Apache Spark feature release added, from **3.0.0 (June 2020)** to
**4.3.0 (October 2026)**. Every release folder has a feature README plus runnable examples in
**PySpark, Scala and Java**.

## Releases at a glance

| Folder | Released | Headline features |
|---|---|---|
| [`spark3.0.0`](./spark3.0.0/README.md) | June 2020 | Adaptive Query Execution, Dynamic Partition Pruning, New join hints, Pandas UDFs with Python type hints |
| [`spark3.1.0`](./spark3.1.0/README.md) | March 2021 | unionByName with allowMissingColumns, Nested struct editing: withField / dropFields, New functions, Streaming to/from catalog tables |
| [`spark3.2.0`](./spark3.2.0/README.md) | October 2021 | Pandas API on Spark, AQE enabled by default, ANSI interval types, Session windows |
| [`spark3.3.0`](./spark3.3.0/README.md) | June 2022 | Runtime Bloom filter join, try_* functions, ILIKE and timestampadd/timestampdiff, Trigger.AvailableNow |
| [`spark3.4.0`](./spark3.4.0/README.md) | April 2023 | Spark Connect, TIMESTAMP_NTZ type, Column DEFAULT values, Lateral column alias |
| [`spark3.5.0`](./spark3.5.0/README.md) | September 2023 | Python UDTFs, Arrow-optimised Python UDFs, Parameterized SQL, IDENTIFIER clause |
| [`spark4.0.0`](./spark4.0.0/README.md) | May 2025 | ANSI mode on by default, VARIANT data type, String collation, SQL user-defined functions |
| [`spark4.1.0`](./spark4.1.0/README.md) | December 2025 | Spark Declarative Pipelines, Real-Time Mode for Structured Streaming, SQL scripting GA, VARIANT GA + shredding + `:` syntax |
| [`spark4.2.0`](./spark4.2.0/README.md) | July 2026 | Geospatial types, Change Data Capture, Auto CDC in Declarative Pipelines, NEAREST BY join + vector functions |
| [`spark4.3.0`](./spark4.3.0/README.md) | October 2026 | Stateful Real-Time Mode, Auto CDC SCD Type 2, ASOF JOIN, JSON_TABLE |

## Project layout

```
spark-release-features/
├── README.md
├── build-templates/          # sbt + Maven builds parameterised by Spark version
├── spark3.0.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark3.1.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark3.2.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark3.3.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark3.4.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark3.5.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark4.0.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark4.1.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark4.2.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
├── spark4.3.0/   README.md · pyspark_examples/ · scala_examples/ · java_examples/
```

Each release folder follows the same shape:

```
sparkX.Y.0/
├── README.md                 # what's new, platform changes, upgrade notes, how to run
├── pyspark_examples/         # spark_X_Y_Z_features.py (+ pipelines/ for 4.1 and 4.3)
├── scala_examples/           # SparkXYZFeatures.scala  (object with main)
└── java_examples/            # SparkXYZFeatures.java   (class with main)
```

## Running

Examples use `local[*]` and in-memory or `/tmp` data, so a laptop is enough.

- **PySpark** — `pip install "pyspark==<version>" pandas pyarrow`, then run the `.py` file.
- **Scala** — `spark-shell -i scala_examples/<File>.scala`, then call `<Object>.main(Array())`; or build with
  `build-templates/build.sbt`.
- **Java** — build with `build-templates/pom.xml` and run with `spark-submit --class <Class>`.

Use a JDK the release supports: Java 8/11 for 3.0–3.2, Java 11/17 for 3.3–3.5, Java 17+ for 4.x.

Every feature demo is wrapped in a small `demo(...)` helper. If a feature needs extra setup (a Kafka broker, a
CDC-capable table format, an optional Python package), the script prints `[skipped] <reason>` and moves on
instead of crashing.

## A few cross-release themes

- **Adaptive execution**: AQE arrives in 3.0, is on by default in 3.2, and later releases keep extending runtime
  re-optimisation (Bloom filter joins in 3.3, single-task execution for small data in 4.3).
- **ANSI SQL**: opt-in in 3.0, `try_*` escape hatches in 3.3, **on by default in 4.0**.
- **Python**: type-hinted Pandas UDFs (3.0) → pandas API on Spark (3.2) → Arrow UDFs and UDTFs (3.5) → Python
  data sources (4.0) → Arrow-native UDFs (4.1) → Arrow by default (4.2) → UDF transpilation (4.3).
- **Client/server**: Spark Connect appears in 3.4, reaches near parity in 4.0, gets a JDBC driver in 4.1 and a REST
  API in 4.3.
- **Streaming**: AvailableNow (3.3) → transformWithState and the state reader (4.0) → Real-Time Mode (4.1) →
  stateful Real-Time Mode (4.3).
- **Data engineering**: Declarative Pipelines (4.1) → CDC and Auto CDC SCD1 (4.2) → Auto CDC SCD2 (4.3).

## Release cadence note

Starting with 4.3, Spark ships feature releases **quarterly**, each maintained for six months; the 4.x
long-term-support release is planned to be 4.5. Spark 3.1's first published build was 3.1.1, so the
`spark3.1.0/` folder targets 3.1.1.

## Sources

Official release notes: https://spark.apache.org/releases/ · Versioning policy:
https://spark.apache.org/versioning-policy.html
