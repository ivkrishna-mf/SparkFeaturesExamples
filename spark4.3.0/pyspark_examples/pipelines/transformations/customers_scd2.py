"""
Auto CDC, SCD Type 2 (Spark 4.3): keep every version of each customer row,
bounded by __START_AT / __END_AT.
"""
from pyspark import pipelines as dp
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr

spark = SparkSession.active()


@dp.temporary_view
def customer_changes():
    """A change feed: one row per insert/update/delete, ordered by seq."""
    return spark.readStream.table("raw.customer_cdc_feed")


dp.create_streaming_table("customers_history")

dp.create_auto_cdc_flow(
    target="customers_history",
    source="customer_changes",
    keys=["customer_id"],
    sequence_by=col("seq"),
    apply_as_deletes=expr("op = 'DELETE'"),
    except_column_list=["op", "seq"],
    stored_as_scd_type=2,                      # new in 4.3 (4.2 supported type 1)
    track_history_column_list=["city", "plan"],  # only these columns open a new version
)
