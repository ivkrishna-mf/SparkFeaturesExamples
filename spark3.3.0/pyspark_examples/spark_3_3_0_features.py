"""
Apache Spark 3.3.0 — feature tour (PySpark)

Covers: runtime Bloom filter joins, try_* functions, ILIKE, timestampadd/timestampdiff,
Trigger.AvailableNow, mapInArrow.

Run:  pip install "pyspark==3.3.0" pandas pyarrow && python spark_3_3_0_features.py
"""
import json
import os
import tempfile

import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder.appName("spark-3.3.0-features").master("local[*]")
    .config("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
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



# 1. Runtime Bloom filter join ------------------------------------------------------
def bloom():
    fact_path = os.path.join(tempfile.mkdtemp(), "fact")
    spark.range(0, 2_000_000).select(F.col("id"), (F.col("id") % 50_000).alias("k")) \
        .write.parquet(fact_path)
    fact = spark.read.parquet(fact_path)
    dim = spark.range(0, 50_000).select(F.col("id").alias("k"), (F.col("id") % 100).alias("grp"))
    q = fact.join(dim.filter("grp = 7"), "k").groupBy("grp").count()
    q.explain()    # look for bloom_filter_agg / might_contain in the plan
    q.show()


demo("Runtime Bloom filter join", bloom)

# 2. try_* functions -----------------------------------------------------------------
sql("try_add / try_divide / try_element_at / try_sum", """
    SELECT try_add(2147483647, 1)              AS overflow_is_null,
           try_divide(10, 0)                   AS div_by_zero_is_null,
           try_element_at(array(1, 2, 3), 10)  AS out_of_bounds_is_null,
           (SELECT try_sum(x) FROM VALUES (1), (2), (3) AS t(x)) AS safe_sum
""")

# 3. ILIKE + timestampadd/timestampdiff ---------------------------------------------------
spark.createDataFrame([("Apache Spark",), ("SPARK SQL",), ("Flink",)], "name string").createOrReplaceTempView("projects")
sql("ILIKE", "SELECT name FROM projects WHERE name ILIKE '%spark%'")
sql("timestampadd / timestampdiff", """
    SELECT timestampadd(HOUR, 36, TIMESTAMP'2022-06-16 00:00:00')                          AS plus_36h,
           timestampdiff(DAY, TIMESTAMP'2022-01-01 00:00:00', TIMESTAMP'2022-06-16 00:00:00') AS days_between
""")


# 4. Trigger.AvailableNow ---------------------------------------------------------------------
def available_now():
    src = tempfile.mkdtemp()
    for i in range(3):
        with open(os.path.join(src, f"part-{i}.json"), "w") as fh:
            fh.write("\n".join(json.dumps({"id": i * 10 + j, "v": j}) for j in range(5)))
    q = (spark.readStream.schema("id long, v long").option("maxFilesPerTrigger", 1).json(src)
         .writeStream.format("memory").queryName("backfill")
         .trigger(availableNow=True).start())
    q.awaitTermination()      # stops on its own once all current files are processed
    print("rows:", spark.table("backfill").count(), "| batches:", len(q.recentProgress))


demo("Trigger.AvailableNow", available_now)


# 5. mapInArrow ---------------------------------------------------------------------------------
def double_amount(batches):
    for b in batches:
        yield pa.RecordBatch.from_arrays(
            [b.column(0), pc.multiply(b.column(1), 2)], names=["id", "amount"])


demo("mapInArrow", lambda: (
    spark.range(5).select("id", (F.col("id") * 1.5).alias("amount"))
    .mapInArrow(double_amount, "id long, amount double").show()))

spark.stop()
