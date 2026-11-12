"""
Apache Spark 4.0.0 — feature tour (PySpark)

Covers: ANSI default, VARIANT, collation, SQL UDF, pipe syntax, SQL scripting (preview),
Python Data Source API, DataFrame.plot.

Run:  pip install "pyspark==4.0.0" pandas pyarrow plotly && python spark_4_0_0_features.py
Tip:  .config("spark.api.mode", "connect") runs the same code through Spark Connect.
"""
from pyspark.sql import SparkSession
from pyspark.sql.datasource import DataSource, DataSourceReader

spark = SparkSession.builder.appName("spark-4.0.0-features").master("local[*]").getOrCreate()
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



# 1. ANSI mode is now the default ---------------------------------------------------------
def ansi_default():
    print("spark.sql.ansi.enabled =", spark.conf.get("spark.sql.ansi.enabled"))
    try:
        spark.sql("SELECT 1 / 0").collect()
    except Exception as e:  # noqa: BLE001
        print("1/0 raised:", str(e).splitlines()[0][:80])
    spark.sql("SELECT try_divide(1, 0) AS safe, try_cast('abc' AS INT) AS safe_cast").show()


demo("ANSI default", ansi_default)

# 2. VARIANT ------------------------------------------------------------------------------------
sql("VARIANT: parse_json + variant_get", """
    WITH events AS (
        SELECT parse_json('{"user": {"id": 7, "tags": ["new", "mobile"]}, "amount": 42.5}') AS v
    )
    SELECT variant_get(v, '$.user.id', 'int')          AS user_id,
           variant_get(v, '$.user.tags[0]', 'string')  AS first_tag,
           try_variant_get(v, '$.missing', 'int')      AS missing,
           schema_of_variant(v)                        AS inferred_schema
    FROM events
""")

# 3. Collation ------------------------------------------------------------------------------------
sql("Collation", """
    SELECT 'Spark' = 'SPARK'                          AS binary_eq,
           'Spark' = 'SPARK' COLLATE UTF8_LCASE       AS lcase_eq,
           collation('x' COLLATE UNICODE_CI)          AS coll_name
""")

# 4. SQL UDF ---------------------------------------------------------------------------------------
demo("SQL UDF", lambda: (
    spark.sql("CREATE OR REPLACE TEMPORARY FUNCTION to_fahrenheit(c DOUBLE) RETURNS DOUBLE RETURN c * 9 / 5 + 32"),
    spark.sql("SELECT c, to_fahrenheit(c) AS f FROM VALUES (0.0), (37.0), (100.0) AS t(c)").show()))

# 5. Pipe syntax --------------------------------------------------------------------------------------
spark.createDataFrame(
    [("c1", 30.0), ("c1", 70.0), ("c2", 55.0), ("c3", 90.0), ("c3", 15.0)], "cust string, amount double"
).createOrReplaceTempView("orders")
sql("Pipe syntax |>", """
    FROM orders
    |> WHERE amount > 20
    |> AGGREGATE sum(amount) AS total, count(*) AS n GROUP BY cust
    |> ORDER BY total DESC
""")


# 6. SQL scripting (preview in 4.0) ------------------------------------------------------------------
def scripting():
    spark.conf.set("spark.sql.scripting.enabled", "true")
    spark.sql("""
        BEGIN
          DECLARE total INT DEFAULT 0;
          DECLARE i INT DEFAULT 1;
          WHILE i <= 5 DO
            SET total = total + i;
            SET i = i + 1;
          END WHILE;
          SELECT total AS sum_1_to_5;
        END
    """).show()


demo("SQL scripting", scripting)


# 7. Python Data Source API --------------------------------------------------------------------------
class PlanetsDataSource(DataSource):
    @classmethod
    def name(cls):
        return "planets"

    def schema(self):
        return "name string, moons int"

    def reader(self, schema):
        return PlanetsReader()


class PlanetsReader(DataSourceReader):
    def read(self, partition):
        yield ("Mercury", 0)
        yield ("Earth", 1)
        yield ("Mars", 2)
        yield ("Jupiter", 95)


def python_data_source():
    spark.dataSource.register(PlanetsDataSource)
    spark.read.format("planets").load().filter("moons > 0").show()


demo("Python Data Source API", python_data_source)


# 8. DataFrame.plot (Plotly backend) -------------------------------------------------------------------
def plotting():
    fig = spark.table("orders").groupBy("cust").sum("amount").plot.bar(x="cust", y="sum(amount)")
    print("Built a", type(fig).__name__, "— call fig.show() in a notebook")


demo("DataFrame.plot", plotting)

spark.stop()
