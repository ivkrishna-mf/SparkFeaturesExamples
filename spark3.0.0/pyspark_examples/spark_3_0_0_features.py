"""
Apache Spark 3.0.0 — feature tour (PySpark)

Covers: AQE, formatted EXPLAIN, new join hints, type-hinted Pandas UDFs, mapInPandas,
cogroup().applyInPandas(), new SQL functions, ANSI mode, binaryFile source.

Run:  pip install "pyspark==3.0.0" pandas pyarrow && python spark_3_0_0_features.py
"""
import os

import pandas as pd
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import pandas_udf

spark = (
    SparkSession.builder.appName("spark-3.0.0-features")
    .master("local[*]")
    # AQE is new in 3.0 but OFF by default (it becomes the default in 3.2)
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
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


orders = spark.range(0, 200_000).select(
    F.col("id").alias("order_id"),
    (F.col("id") % 1_000).alias("cust_id"),
    (F.rand(seed=42) * 100).alias("amount"),
)
customers = spark.range(0, 1_000).select(
    F.col("id").alias("cust_id"),
    F.when(F.col("id") % 3 == 0, "gold").otherwise("silver").alias("tier"),
)
orders.createOrReplaceTempView("orders")
customers.createOrReplaceTempView("customers")


# 1. Adaptive Query Execution + the new "formatted" explain mode -------------
def aqe():
    by_tier = (orders.join(customers, "cust_id")
               .groupBy("tier").agg(F.sum("amount").alias("revenue")))
    by_tier.explain("formatted")       # look for "AdaptiveSparkPlan isFinalPlan=..."
    by_tier.show()


demo("AQE + EXPLAIN FORMATTED", aqe)

# 2. New join hints: MERGE, SHUFFLE_HASH, SHUFFLE_REPLICATE_NL ---------------
demo("SHUFFLE_HASH hint (DataFrame API)",
     lambda: orders.join(customers.hint("shuffle_hash"), "cust_id").explain())
sql("MERGE hint (SQL)", """
    SELECT /*+ MERGE(o) */ c.tier, count(*) AS orders
    FROM orders o JOIN customers c ON o.cust_id = c.cust_id
    GROUP BY c.tier
""")


# 3. Pandas UDF driven by Python type hints -----------------------------------
@pandas_udf("double")
def with_tax(amount: pd.Series) -> pd.Series:
    return amount * 1.2


demo("Type-hinted pandas UDF",
     lambda: orders.select("order_id", "amount", with_tax("amount").alias("with_tax")).show(3))


# 4. mapInPandas: stream of pandas DataFrames in, stream out -------------------
def big_orders(batches):
    for pdf in batches:
        yield pdf[pdf.amount > 95]


demo("mapInPandas", lambda: orders.mapInPandas(big_orders, schema=orders.schema).show(3))

# 5. Co-grouped map --------------------------------------------------------------
left = spark.createDataFrame([(1, "a"), (2, "b")], "id long, l string")
right = spark.createDataFrame([(1, 10.0), (1, 20.0), (3, 30.0)], "id long, r double")


def merge_groups(l: pd.DataFrame, r: pd.DataFrame) -> pd.DataFrame:
    key = l.id.iloc[0] if len(l) else r.id.iloc[0]
    return pd.DataFrame({"id": [key], "left_rows": [len(l)], "right_sum": [float(r.r.sum())]})


demo("cogroup().applyInPandas()", lambda: (
    left.groupby("id").cogroup(right.groupby("id"))
    .applyInPandas(merge_groups, schema="id long, left_rows long, right_sum double")
    .orderBy("id").show()))

# 6. New built-in SQL functions -------------------------------------------------
sql("count_if / max_by / bool_and / make_date", """
    SELECT count_if(amount > 50)    AS big_orders,
           max_by(order_id, amount) AS top_order,
           bool_and(amount >= 0)    AS all_non_negative,
           make_date(2020, 6, 18)   AS spark3_ga
    FROM orders
""")


# 7. ANSI SQL mode ----------------------------------------------------------------
def ansi():
    spark.conf.set("spark.sql.ansi.enabled", "false")
    print("ANSI off ->", spark.sql("SELECT 2147483647 + 1 AS wrapped").first())
    spark.conf.set("spark.sql.ansi.enabled", "true")
    try:
        spark.sql("SELECT 2147483647 + 1 AS overflow").collect()
    except Exception as e:  # noqa: BLE001
        print("ANSI on  -> raised", type(e).__name__)
    finally:
        spark.conf.set("spark.sql.ansi.enabled", "false")


demo("ANSI mode", ansi)

# 8. binaryFile data source ---------------------------------------------------------
here = os.path.dirname(os.path.abspath(__file__))
demo("binaryFile source", lambda: (
    spark.read.format("binaryFile").option("pathGlobFilter", "*.py").load(here)
    .select("path", "length", "modificationTime").show(truncate=False)))

spark.stop()
