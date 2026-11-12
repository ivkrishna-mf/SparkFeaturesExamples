"""
Apache Spark 3.2.0 — feature tour (PySpark)

Covers: pandas API on Spark, AQE default-on, ANSI intervals, session_window, LATERAL subquery,
RocksDB state store.

Run:  pip install "pyspark==3.2.0" pandas pyarrow && python spark_3_2_0_features.py
"""
import pyspark.pandas as ps
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("spark-3.2.0-features").master("local[*]")
    .config("spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
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



# 1. Pandas API on Spark ---------------------------------------------------------
def pandas_on_spark():
    psdf = ps.DataFrame({
        "city": ["Pune", "Pune", "Delhi", "Delhi", "Chennai"],
        "temp_c": [24.0, 27.5, 31.0, 33.5, 30.0],
    })
    psdf["temp_f"] = psdf.temp_c * 9 / 5 + 32
    print(psdf.groupby("city").temp_f.mean().sort_index())
    sdf = psdf.to_spark()                 # pandas-on-Spark -> Spark DataFrame
    back = sdf.to_pandas_on_spark()       # and back again (renamed pandas_api() in 3.3)
    print(type(back))


demo("pandas API on Spark", pandas_on_spark)

# 2. AQE is on by default ----------------------------------------------------------
demo("AQE default", lambda: print("spark.sql.adaptive.enabled =", spark.conf.get("spark.sql.adaptive.enabled")))

# 3. ANSI interval types --------------------------------------------------------------
sql("Year-month and day-time intervals", """
    SELECT INTERVAL '1-6' YEAR TO MONTH                       AS ym,
           INTERVAL '2 03:04:05' DAY TO SECOND                AS dt,
           DATE'2021-10-13' + INTERVAL '1-6' YEAR TO MONTH    AS eighteen_months_later,
           TIMESTAMP'2021-10-13 12:00:00' - TIMESTAMP'2021-10-12 08:30:00' AS elapsed
""")

# 4. Session windows ----------------------------------------------------------------------
clicks = spark.createDataFrame(
    [("u1", "2021-10-13 10:00:00"), ("u1", "2021-10-13 10:03:00"),
     ("u1", "2021-10-13 10:20:00"), ("u2", "2021-10-13 10:01:00")],
    "user string, ts string").withColumn("ts", F.to_timestamp("ts"))
demo("session_window (5 minute gap)", lambda: (
    clicks.groupBy("user", F.session_window("ts", "5 minutes")).count()
    .orderBy("user", "session_window").show(truncate=False)))

# 5. LATERAL subquery --------------------------------------------------------------------
spark.createDataFrame([("c1",), ("c2",)], "cust string").createOrReplaceTempView("customers")
spark.createDataFrame([("c1", 10), ("c1", 40), ("c2", 25)], "cust string, amount int").createOrReplaceTempView("orders")
sql("LATERAL subquery", """
    SELECT c.cust, t.max_amount
    FROM customers c,
         LATERAL (SELECT max(o.amount) AS max_amount FROM orders o WHERE o.cust = c.cust) t
""")


# 6. Stateful streaming backed by RocksDB ------------------------------------------------
def rocksdb_state():
    q = (spark.readStream.format("rate").option("rowsPerSecond", 50).load()
         .groupBy((F.col("value") % 5).alias("bucket")).count()
         .writeStream.format("memory").queryName("bucket_counts").outputMode("complete").start())
    q.awaitTermination(8)
    q.stop()
    spark.table("bucket_counts").orderBy("bucket").show()


demo("RocksDB state store provider", rocksdb_state)

spark.stop()
