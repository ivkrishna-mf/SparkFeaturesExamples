"""
Apache Spark 3.1 — feature tour (PySpark)

Covers: unionByName(allowMissingColumns), withField/dropFields, nth_value, percentile_approx,
regexp_extract_all, timestamp_seconds, current_timezone, CHAR/VARCHAR, streaming toTable().

Run:  pip install "pyspark==3.1.1" && python spark_3_1_features.py
"""
import tempfile

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("spark-3.1-features").master("local[*]").getOrCreate()
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


# 1. unionByName with allowMissingColumns -----------------------------------------
jan = spark.createDataFrame([(1, "alice", 10.0)], "id int, name string, amount double")
feb = spark.createDataFrame([(2, "bob", "EU")], "id int, name string, region string")
demo("unionByName(allowMissingColumns=True)",
     lambda: jan.unionByName(feb, allowMissingColumns=True).show())

# 2. withField / dropFields on structs -----------------------------------------------
people = spark.createDataFrame(
    [(1, ("Main St", "Pune", "411001"))],
    "id int, address struct<street:string, city:string, zip:string>")
demo("withField / dropFields", lambda: people.select(
    "id",
    F.col("address").withField("country", F.lit("IN")).alias("with_country"),
    F.col("address").dropFields("zip").alias("without_zip"),
).show(truncate=False))

# 3. New functions ---------------------------------------------------------------------
sales = spark.createDataFrame(
    [("north", 1, 100), ("north", 2, 300), ("north", 3, 200), ("south", 1, 50), ("south", 2, 80)],
    "region string, month int, revenue int")
w = (Window.partitionBy("region").orderBy("month")
     .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing))
demo("nth_value window function",
     lambda: sales.select("*", F.nth_value("revenue", 2).over(w).alias("month2_revenue")).show())
demo("percentile_approx in the Python API",
     lambda: sales.groupBy("region").agg(F.percentile_approx("revenue", 0.5).alias("median")).show())
sql("regexp_extract_all / timestamp_seconds / current_timezone", r"""
    SELECT regexp_extract_all('order-12, order-34, order-56', 'order-(\\d+)', 1) AS ids,
           timestamp_seconds(1609459200) AS new_year_2021,
           current_timezone()            AS tz
""")


# 4. CHAR / VARCHAR semantics -------------------------------------------------------------
def char_varchar():
    spark.sql("DROP TABLE IF EXISTS codes")
    spark.sql("CREATE TABLE codes (code CHAR(5), label VARCHAR(8)) USING parquet")
    spark.sql("INSERT INTO codes VALUES ('ab', 'short')")
    spark.sql("SELECT code, length(code) AS char_len, label FROM codes").show()
    try:
        spark.sql("INSERT INTO codes VALUES ('cd', 'way-too-long-label')")
    except Exception as e:  # noqa: BLE001
        print("VARCHAR(8) length check raised", type(e).__name__)


demo("CHAR / VARCHAR", char_varchar)


# 5. Streaming into a catalog table with toTable() -----------------------------------------
def stream_to_table():
    spark.sql("DROP TABLE IF EXISTS rate_events")
    q = (spark.readStream.format("rate").option("rowsPerSecond", 20).load()
         .writeStream.option("checkpointLocation", tempfile.mkdtemp())
         .toTable("rate_events"))
    q.awaitTermination(8)
    q.stop()
    spark.table("rate_events").orderBy("value").show(5)
    print("readStream.table() is streaming:", spark.readStream.table("rate_events").isStreaming)


demo("DataStreamWriter.toTable / DataStreamReader.table", stream_to_table)

spark.stop()
