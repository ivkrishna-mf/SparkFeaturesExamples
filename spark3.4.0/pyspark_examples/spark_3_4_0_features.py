"""
Apache Spark 3.4.0 — feature tour (PySpark)

Covers: Spark Connect (snippet), TIMESTAMP_NTZ, DEFAULT column values, lateral column alias,
unpivot/melt, OFFSET, array_append/array_insert/array_compact, withColumnsRenamed.

Run:  pip install "pyspark==3.4.0" pandas pyarrow && python spark_3_4_0_features.py
"""
from pyspark.sql import SparkSession, functions as F

# --- Spark Connect: same DataFrame API, remote execution ---------------------------
# Start a server:  ./sbin/start-connect-server.sh --packages org.apache.spark:spark-connect_2.12:3.4.0
# Then (pip install "pyspark[connect]==3.4.0"):
#   spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark = SparkSession.builder.appName("spark-3.4.0-features").master("local[*]").getOrCreate()
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


# 1. TIMESTAMP_NTZ ------------------------------------------------------------------
sql("TIMESTAMP_NTZ vs TIMESTAMP_LTZ", """
    SELECT TIMESTAMP_NTZ'2023-04-13 09:00:00'         AS wall_clock,
           typeof(TIMESTAMP_NTZ'2023-04-13 09:00:00') AS ntz_type,
           typeof(TIMESTAMP'2023-04-13 09:00:00')     AS ltz_type
""")


# 2. DEFAULT column values --------------------------------------------------------------
def defaults():
    spark.sql("DROP TABLE IF EXISTS employees")
    spark.sql("CREATE TABLE employees (id INT, name STRING, status STRING DEFAULT 'active') USING parquet")
    spark.sql("INSERT INTO employees (id, name) VALUES (1, 'asha')")
    spark.sql("INSERT INTO employees VALUES (2, 'ravi', DEFAULT)")
    spark.table("employees").orderBy("id").show()


demo("DEFAULT column values", defaults)

# 3. Lateral column alias ------------------------------------------------------------------
sql("Lateral column alias", """
    SELECT price, price * 0.18 AS gst, price + gst AS total
    FROM VALUES (100.0), (250.0) AS t(price)
""")

# 4. unpivot / melt ------------------------------------------------------------------------
wide = spark.createDataFrame([(1, 100, 120), (2, 80, 95)], "store int, q1 int, q2 int")
demo("DataFrame.unpivot", lambda: wide.unpivot("store", ["q1", "q2"], "quarter", "revenue").show())
wide.createOrReplaceTempView("wide")
sql("SQL UNPIVOT", "SELECT * FROM wide UNPIVOT (revenue FOR quarter IN (q1, q2))")

# 5. OFFSET -----------------------------------------------------------------------------------
sql("LIMIT ... OFFSET", "SELECT id FROM range(10) ORDER BY id LIMIT 3 OFFSET 4")
demo("DataFrame.offset", lambda: spark.range(10).offset(7).show())

# 6. New array functions + bulk rename --------------------------------------------------------
sql("array_append / array_insert / array_compact", """
    SELECT array_append(array(1, 2), 3)        AS appended,
           array_insert(array(1, 3), 2, 2)     AS inserted,
           array_compact(array(1, NULL, 2))    AS compacted
""")
demo("withColumnsRenamed", lambda: wide.withColumnsRenamed({"q1": "quarter_1", "q2": "quarter_2"}).show())

spark.stop()
