"""
Apache Spark 4.1.0 — feature tour (PySpark)

Covers: SQL scripting GA, recursive CTE, VARIANT `:` syntax, approx_top_k / try_to_date,
Arrow-native UDF, Python Data Source filter pushdown.
Declarative Pipelines live in ./pipelines (run with `spark-pipelines run`).

Run:  pip install "pyspark==4.1.0" pandas pyarrow && python spark_4_1_0_features.py
"""
import pyarrow as pa
import pyarrow.compute as pc
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.datasource import DataSource, DataSourceReader, EqualTo

spark = (
    SparkSession.builder.appName("spark-4.1.0-features").master("local[*]")
    .config("spark.sql.python.filterPushdown.enabled", "true")
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


# 1. SQL scripting (GA, on by default) ---------------------------------------------------
sql("SQL scripting with multi-variable DECLARE", """
    BEGIN
      DECLARE total, i INT DEFAULT 0;
      WHILE i < 5 DO
        SET i = i + 1;
        SET total = total + i * i;
      END WHILE;
      SELECT total AS sum_of_squares_1_to_5;
    END
""")

# 2. Recursive CTE -------------------------------------------------------------------------
spark.createDataFrame(
    [("ceo", None), ("vp_eng", "ceo"), ("vp_sales", "ceo"), ("eng_mgr", "vp_eng"), ("engineer", "eng_mgr")],
    "employee string, manager string").createOrReplaceTempView("org")
sql("WITH RECURSIVE (org chart depth)", """
    WITH RECURSIVE chain(employee, depth) AS (
        SELECT employee, 0 FROM org WHERE manager IS NULL
        UNION ALL
        SELECT o.employee, c.depth + 1 FROM org o JOIN chain c ON o.manager = c.employee
    )
    SELECT * FROM chain ORDER BY depth
""")

# 3. VARIANT GA with colon path syntax ------------------------------------------------------
sql("VARIANT `:` path syntax", """
    SELECT v:user.id::int        AS user_id,
           v:user.plan::string   AS plan,
           v:items[0].sku        AS first_sku
    FROM (SELECT parse_json('{"user":{"id":11,"plan":"pro"},"items":[{"sku":"A-1"},{"sku":"B-2"}]}') AS v)
""")

# 4. New functions ----------------------------------------------------------------------------
sql("approx_top_k + try_to_date", """
    SELECT approx_top_k(item, 2) AS top_items, try_to_date('2025-13-45') AS bad_date_is_null
    FROM VALUES ('tea'), ('tea'), ('coffee'), ('tea'), ('juice'), ('coffee') AS t(item)
""")


# 5. Arrow-native UDF (no pandas in the loop) ----------------------------------------------------
def arrow_udf_demo():
    from pyspark.sql.functions import arrow_udf

    @arrow_udf("double")
    def add_tax(price: pa.Array) -> pa.Array:
        return pc.multiply(price, 1.18)

    spark.range(1, 4).select((F.col("id") * 100.0).alias("price")) \
        .select("price", add_tax("price").alias("with_tax")).show()


demo("@arrow_udf", arrow_udf_demo)


# 6. Python Data Source with filter pushdown ----------------------------------------------------
class CitiesDataSource(DataSource):
    @classmethod
    def name(cls):
        return "cities"

    def schema(self):
        return "country string, city string"

    def reader(self, schema):
        return CitiesReader()


class CitiesReader(DataSourceReader):
    DATA = [("IN", "Pune"), ("IN", "Delhi"), ("FR", "Paris"), ("JP", "Osaka")]

    def __init__(self):
        self.country = None

    def pushFilters(self, filters):
        # Keep the filters we can apply at the source; yield the rest back to Spark.
        for f in filters:
            if isinstance(f, EqualTo) and f.attribute == ("country",):
                self.country = f.value
            else:
                yield f

    def read(self, partition):
        for row in self.DATA:
            if self.country is None or row[0] == self.country:
                yield row


def pushdown_demo():
    spark.dataSource.register(CitiesDataSource)
    spark.read.format("cities").load().filter("country = 'IN'").show()


demo("Python Data Source filter pushdown", pushdown_demo)

spark.stop()
