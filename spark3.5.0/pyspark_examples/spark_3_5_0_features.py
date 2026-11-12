"""
Apache Spark 3.5.0 — feature tour (PySpark)

Covers: Python UDTFs, Arrow-optimised UDFs, parameterized SQL, IDENTIFIER clause, HLL sketches,
array_prepend, assertDataFrameEqual, dropDuplicatesWithinWatermark.

Run:  pip install "pyspark==3.5.0" pandas pyarrow && python spark_3_5_0_features.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf, udtf

spark = SparkSession.builder.appName("spark-3.5.0-features").master("local[*]").getOrCreate()
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



# 1. Python UDTF -----------------------------------------------------------------------
@udtf(returnType="word: string, length: int")
class SplitWords:
    def eval(self, text: str):
        for w in text.split():
            yield w, len(w)


demo("UDTF via DataFrame API", lambda: SplitWords(F.lit("spark three point five")).show())
spark.udtf.register("split_words", SplitWords)
sql("UDTF via SQL", "SELECT * FROM split_words('table functions in python')")


# 2. Arrow-optimised Python UDF ---------------------------------------------------------
@udf(returnType="int", useArrow=True)
def str_len(s):
    return len(s) if s is not None else None


demo("useArrow=True UDF", lambda: spark.createDataFrame([("spark",), ("arrow",)], "s string")
     .select("s", str_len("s").alias("len")).show())

# 3. Parameterized SQL: named and positional ---------------------------------------------
demo("Named parameters", lambda: spark.sql(
    "SELECT id FROM range(10) WHERE id BETWEEN :lo AND :hi", args={"lo": 3, "hi": 5}).show())
demo("Positional parameters", lambda: spark.sql(
    "SELECT id FROM range(10) WHERE id > ? AND id % ? = 0", args=[4, 2]).show())

# 4. IDENTIFIER clause ---------------------------------------------------------------------
spark.range(3).createOrReplaceTempView("demo_tbl")
demo("IDENTIFIER(:tbl)", lambda: spark.sql(
    "SELECT * FROM IDENTIFIER(:tbl)", args={"tbl": "demo_tbl"}).show())

# 5. HLL sketches ----------------------------------------------------------------------------
sql("hll_sketch_agg / hll_union_agg", """
    WITH daily AS (
        SELECT day, hll_sketch_agg(user_id) AS sketch
        FROM VALUES (1, 'a'), (1, 'b'), (2, 'b'), (2, 'c'), (2, 'd') AS t(day, user_id)
        GROUP BY day
    )
    SELECT hll_sketch_estimate(hll_union_agg(sketch)) AS distinct_users_all_days FROM daily
""")

# 6. array_prepend -----------------------------------------------------------------------------
sql("array_prepend", "SELECT array_prepend(array(2, 3), 1) AS xs")


# 7. Testing utilities ---------------------------------------------------------------------------
def testing():
    from pyspark.testing import assertDataFrameEqual
    actual = spark.createDataFrame([(1, "a"), (2, "b")], "id int, v string")
    expected = spark.createDataFrame([(2, "b"), (1, "a")], "id int, v string")
    assertDataFrameEqual(actual, expected)     # row order ignored by default
    print("assertDataFrameEqual passed")


demo("assertDataFrameEqual", testing)


# 8. dropDuplicatesWithinWatermark ---------------------------------------------------------------
def dedup():
    q = (spark.readStream.format("rate").option("rowsPerSecond", 20).load()
         .withColumn("event_id", F.col("value") % 7)
         .withWatermark("timestamp", "10 seconds")
         .dropDuplicatesWithinWatermark(["event_id"])
         .writeStream.format("memory").queryName("dedup").start())
    q.awaitTermination(8)
    q.stop()
    spark.table("dedup").orderBy("event_id").show()


demo("dropDuplicatesWithinWatermark", dedup)

spark.stop()
