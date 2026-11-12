"""
Spark Declarative Pipelines (SDP) — Python definitions.

You declare datasets; Spark works out dependency order, parallelism, checkpoints and retries.
Requires:  pip install "pyspark[pipelines]==4.1.0"
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.active()


@dp.materialized_view
def raw_orders():
    """Batch source — in real life this would read files or a table."""
    return spark.createDataFrame(
        [(1, "c1", 120.0), (2, "c2", 35.5), (3, "c1", 80.0), (4, "c3", 15.0)],
        "order_id int, cust_id string, amount double",
    )


@dp.materialized_view
def customer_revenue():
    """Depends on raw_orders; SDP infers the edge from spark.table()."""
    return (spark.table("raw_orders")
            .groupBy("cust_id")
            .agg(F.sum("amount").alias("revenue"), F.count("*").alias("orders")))


@dp.table
def order_events():
    """A streaming table fed by a streaming source."""
    return spark.readStream.format("rate").option("rowsPerSecond", 5).load()
