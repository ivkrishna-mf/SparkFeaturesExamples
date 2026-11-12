"""
Apache Spark 4.3.0 — feature tour (PySpark)

Covers: ASOF JOIN, JSON_TABLE, UNNEST WITH ORDINALITY, VARIANT editing functions,
nanosecond timestamps (preview), new functions, DataFrame.zip, single-task execution,
Arrow-native caching, Python UDF transpilation.
Auto CDC SCD Type 2 lives in ./pipelines.

Run:  pip install "pyspark==4.3.0" pandas pyarrow && python spark_4_3_0_features.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf

spark = (
    SparkSession.builder.appName("spark-4.3.0-features").master("local[*]")
    # static conf: Arrow-native in-memory caching (opt-in)
    .config("spark.sql.cache.serializer",
            "org.apache.spark.sql.execution.columnar.ArrowCachedBatchSerializer")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

def demo(title, fn):
    """Run one feature demo; print [skipped] instead of failing the whole script."""
    print(f"\n=== {title} ===")
    try:
        fn()
    except Exception as e:  # noqa: BLE001 - demo script
        msg = str(e).strip().splitlines()[0] if str(e).strip() else ""
        print(f"[skipped] {type(e).__name__}: {msg}")


def sql(title, query):
    demo(title, lambda: spark.sql(query).show(truncate=False))


# 1. ASOF JOIN -------------------------------------------------------------------------------
spark.conf.set("spark.sql.join.asofJoin.enabled", "true")
spark.createDataFrame(
    [("INR", 0, 83.0), ("INR", 7, 83.4), ("EUR", 0, 0.92), ("EUR", 9, 0.91)],
    "ccy string, ts int, rate double").createOrReplaceTempView("fx_rates")
spark.createDataFrame(
    [(1, "INR", 5, 100.0), (2, "INR", 11, 250.0), (3, "EUR", 4, 80.0), (4, "EUR", 12, 40.0)],
    "order_id int, ccy string, ts int, usd double").createOrReplaceTempView("orders")
sql("ASOF JOIN: price each order with the FX rate in force at that time", """
    SELECT o.order_id, o.ccy, o.ts, r.ts AS rate_ts, r.rate, o.usd * r.rate AS local_amount
    FROM orders o
    LEFT ASOF JOIN fx_rates r
      MATCH_CONDITION (o.ts >= r.ts)
      ON o.ccy = r.ccy
    ORDER BY o.order_id
""")

# 2. JSON_TABLE --------------------------------------------------------------------------------
sql("JSON_TABLE", """
    SELECT *
    FROM JSON_TABLE(
      '{"items":[{"sku":"A","qty":2},{"sku":"B","qty":1},{"sku":"C","qty":5}]}',
      '$.items[*]'
      COLUMNS (
        position FOR ORDINALITY,
        sku STRING,
        qty INT
      )
    )
    ORDER BY position
""")

# 3. UNNEST WITH ORDINALITY ------------------------------------------------------------------------
sql("UNNEST (parallel arrays, NULL-padded)", """
    SELECT * FROM UNNEST(array('A', 'B'), array(2, 1, 5)) WITH ORDINALITY AS t(sku, qty, pos)
""")

# 4. VARIANT editing -------------------------------------------------------------------------------
sql("variant_set / variant_delete / variant_strip_nulls", """
    WITH doc AS (SELECT parse_json('{"id": 1, "status": "new", "tmp": "x", "note": null}') AS v)
    SELECT to_json(variant_set(v, '$.status', 'shipped'))  AS set_status,
           to_json(variant_delete(v, '$.tmp'))              AS without_tmp,
           to_json(variant_strip_nulls(v))                  AS no_nulls
    FROM doc
""")


# 5. Nanosecond timestamps (preview) ----------------------------------------------------------------
def nanos():
    spark.conf.set("spark.sql.timestampNanosTypes.enabled", "true")
    spark.sql("""
        SELECT CAST('2026-10-30 12:00:00.123456789' AS TIMESTAMP_NTZ(9)) AS ts_ns,
               typeof(CAST('2026-10-30 12:00:00.123456789' AS TIMESTAMP_NTZ(9))) AS t
    """).show(truncate=False)


demo("TIMESTAMP_NTZ(9)", nanos)

# 6. New functions -------------------------------------------------------------------------------------
sql("jaro_winkler_similarity / to_base32", """
    SELECT jaro_winkler_similarity('martha', 'marhta') AS jw,
           to_base32('spark')                          AS b32,
           CAST(from_base32(to_base32('spark')) AS STRING) AS round_trip
""")

# 7. DataFrame.zip ---------------------------------------------------------------------------------------
base = spark.table("orders")
demo("DataFrame.zip", lambda: base.select("order_id").zip(base.select(F.upper("ccy").alias("CCY"))).show())


# 8. Faster small-data iteration ----------------------------------------------------------------------------
def small_data():
    spark.conf.set("spark.sql.optimizer.singleTaskExecution.enabled", "true")
    q = spark.table("orders").groupBy("ccy").agg(F.sum("usd").alias("usd"))
    q.cache().count()          # cached via ArrowCachedBatchSerializer
    q.explain()                # small single-partition fragments can skip the shuffle
    q.show()


demo("Single-task execution + Arrow cache", small_data)


# 9. Python UDF transpilation (experimental) --------------------------------------------------------------
def transpile():
    spark.conf.set("spark.sql.experimental.optimizer.transpilePyUDFs", "true")

    @udf("int", useArrow=False)          # transpilation applies to non-Arrow UDFs
    def name_len(s):
        return len(s)

    q = spark.table("orders").select(name_len("ccy").alias("n"))
    q.explain()                          # no BatchEvalPython node when transpiled
    q.show(2)


demo("Python UDF -> Catalyst transpilation", transpile)

spark.stop()
