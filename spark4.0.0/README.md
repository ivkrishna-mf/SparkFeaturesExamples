# Apache Spark 4.0.0 — What's New, with Examples

**Released:** May 2025  
**Official release notes:** https://spark.apache.org/releases/spark-release-4-0-0.html

Spark 4.0 is the first major release since 3.0. **ANSI SQL mode is now the default**, a new **VARIANT**
type handles semi-structured data natively, strings gain **collations**, SQL gets **SQL UDFs**, the
`|>` **pipe syntax** and a preview of **SQL scripting**. PySpark adds a **Python Data Source API** and
DataFrame plotting, Structured Streaming gets the arbitrary-state operator **transformWithState** and a
**state data source** reader, and Spark Connect reaches near feature-parity with classic Spark.

## Platform & compatibility

- Java 17 minimum (Java 21 supported); Java 8/11 dropped.
- Scala 2.13 only (Scala 2.12 dropped).
- Python 3.9+; pandas 2.x; lightweight `pyspark-client` package for Spark Connect-only use.
- `spark.api.mode=connect` switches an application to Spark Connect without code changes.

## Feature overview

| # | Feature | Area | PySpark | Scala | Java |
|---|---|---|:-:|:-:|:-:|
| 1 | ANSI mode on by default | SQL | ✅ | — | ✅ |
| 2 | VARIANT data type | SQL | ✅ | ✅ | ✅ |
| 3 | String collation | SQL | ✅ | ✅ | ✅ |
| 4 | SQL user-defined functions | SQL | ✅ | — | ✅ |
| 5 | SQL pipe syntax | SQL | ✅ | ✅ | ✅ |
| 6 | SQL scripting (preview) | SQL | ✅ | — | — |
| 7 | Python Data Source API | PySpark | ✅ | — | — |
| 8 | transformWithState | Structured Streaming | — | ✅ | — |
| 9 | State data source reader | Structured Streaming | — | ✅ | — |
| 10 | DataFrame plotting, structured logging, Spark Connect parity | PySpark / Ops | ✅ | — | — |

✅ = demonstrated in this folder's example code. — = described here only (usually because it needs a cluster, an external system, or is language-specific).

## Features in detail

### 1. ANSI mode on by default

`spark.sql.ansi.enabled` now defaults to `true`: overflow, divide-by-zero and invalid casts raise errors instead of returning NULL or wrapped values. Use `try_*` functions where NULL is the desired result.

### 2. VARIANT data type

Stores JSON-like data in an efficient binary encoding with fast path extraction — no schema up front.

```sql
SELECT variant_get(parse_json(payload), '$.user.id', 'int') FROM events
```

### 3. String collation

`COLLATE UTF8_LCASE`, `UNICODE`, `UNICODE_CI` and locale collations control comparison, sorting and grouping of strings.

```sql
SELECT 'Spark' = 'SPARK' COLLATE UTF8_LCASE  -- true
```

### 4. SQL user-defined functions

Define scalar and table functions in pure SQL with `CREATE FUNCTION ... RETURN ...`; they are inlined by the optimizer.

```sql
CREATE TEMPORARY FUNCTION to_f(c DOUBLE) RETURNS DOUBLE RETURN c * 9 / 5 + 32
```

### 5. SQL pipe syntax

Write queries top-to-bottom with `|>` operators.

```sql
FROM orders
|> WHERE amount > 50
|> AGGREGATE sum(amount) AS total GROUP BY cust
|> ORDER BY total DESC
```

### 6. SQL scripting (preview)

Multi-statement scripts with `BEGIN ... END`, variables, `IF`, `WHILE`. Behind `spark.sql.scripting.enabled` in 4.0; GA in 4.1.

### 7. Python Data Source API

Implement batch/streaming readers and writers in pure Python (`pyspark.sql.datasource.DataSource`) and use them via `spark.read.format(...)`.

### 8. transformWithState

Next-generation arbitrary stateful operator: multiple typed state variables (value, list, map), timers, TTL and schema evolution. Python equivalent: `transformWithStateInPandas`.

### 9. State data source reader

`spark.read.format("statestore").load(checkpoint)` exposes streaming state as a DataFrame for debugging and auditing.

### 10. DataFrame plotting, structured logging, Spark Connect parity

`df.plot.*` renders Plotly charts; `spark.log.structuredLogging.enabled` emits JSON logs; Spark Connect now supports almost all of the DataFrame and MLlib APIs.

## Behaviour changes to watch when upgrading

- ANSI mode default: queries that relied on NULL-on-error behaviour now fail — set `spark.sql.ansi.enabled=false` temporarily while migrating, and prefer `try_cast`/`try_divide` etc.
- Java 17+, Scala 2.13 and Python 3.9+ are required.
- Mesos support removed; legacy `SQLContext`/`HiveContext` usage should move to `SparkSession`.

## Folder layout

```
spark4.0.0/
├── README.md
├── pyspark_examples/
│   └── spark_4_0_0_features.py
├── scala_examples/
│   └── Spark400Features.scala
└── java_examples/
    └── Spark400Features.java
```

## Running the examples

**PySpark**

```bash
pip install "pyspark==4.0.0" pandas pyarrow
python pyspark_examples/spark_4_0_0_features.py
```

**Scala** — quickest via `spark-shell`, or build with the sbt template in `../build-templates`:

```bash
spark-shell --master 'local[*]' -i scala_examples/Spark400Features.scala
scala> Spark400Features.main(Array())

# or package and submit
cd ../build-templates && sbt -Dspark.version=4.0.0 -Dexamples.dir=../spark4.0.0/scala_examples package
spark-submit --class Spark400Features target/scala-2.13/spark-release-examples_2.13-0.1.jar
```

**Java** — build with the Maven template in `../build-templates`:

```bash
cd ../build-templates && mvn -q package -Dspark.version=4.0.0 -Dscala.binary.version=2.13 -Djava.release=17 -Dexamples.dir=../spark4.0.0/java_examples
spark-submit --class Spark400Features target/spark-release-examples-0.1.jar
```

Each example wraps every feature demo in a small `demo(...)` helper that prints a `[skipped]` line instead of crashing if a feature is unavailable in your build or needs extra setup, so one file always runs end to end.
