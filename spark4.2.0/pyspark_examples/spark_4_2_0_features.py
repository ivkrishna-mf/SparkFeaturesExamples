"""
Apache Spark 4.2.0 — feature tour (PySpark)

Covers: geospatial ST_* functions, NEAREST BY + vector similarity, QUALIFY, IGNORE NULLS / max_by k,
SQL PATH, Arrow-by-default UDFs, zipWithIndex, named streaming sources, CDC (snippet).

Run:  pip install "pyspark==4.2.0" pandas pyarrow && python spark_4_2_0_features.py
"""
import tempfile

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.functions import udf

spark = SparkSession.builder.appName("spark-4.2.0-features").master("local[*]").getOrCreate()
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


# 1. Geospatial ----------------------------------------------------------------------------
# WKB for POINT(1 2), little-endian
sql("GEOMETRY from WKB and back", """
    SELECT typeof(g)              AS type,
           hex(ST_AsBinary(g))    AS wkb_hex
    FROM (SELECT ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040')) AS g)
""")

# 2. NEAREST BY + vector functions -----------------------------------------------------------
spark.sql("""
    CREATE OR REPLACE TEMP VIEW docs AS
    SELECT * FROM VALUES
      ('spark-sql',   CAST(array(0.9, 0.1, 0.0) AS ARRAY<FLOAT>)),
      ('spark-ml',    CAST(array(0.2, 0.9, 0.1) AS ARRAY<FLOAT>)),
      ('kafka',       CAST(array(0.1, 0.1, 0.9) AS ARRAY<FLOAT>)),
      ('dataframes',  CAST(array(0.8, 0.3, 0.1) AS ARRAY<FLOAT>))
    AS t(doc, emb)
""")
spark.sql("""
    CREATE OR REPLACE TEMP VIEW queries AS
    SELECT * FROM VALUES ('how do I write sql?', CAST(array(1.0, 0.2, 0.0) AS ARRAY<FLOAT>)) AS t(q, emb)
""")
sql("NEAREST BY (top-2 by cosine similarity)", """
    SELECT q.q, d.doc, vector_cosine_similarity(q.emb, d.emb) AS score
    FROM queries q
    JOIN docs d EXACT NEAREST 2 BY SIMILARITY vector_cosine_similarity(q.emb, d.emb)
""")

# 3. QUALIFY ------------------------------------------------------------------------------------
spark.createDataFrame(
    [("north", "tea", 50), ("north", "coffee", 80), ("south", "tea", 70), ("south", "juice", 20)],
    "region string, product string, revenue int").createOrReplaceTempView("sales")
sql("QUALIFY (best product per region)", """
    SELECT region, product, revenue
    FROM sales
    QUALIFY row_number() OVER (PARTITION BY region ORDER BY revenue DESC) = 1
""")

# 4. IGNORE NULLS + max_by with k ------------------------------------------------------------------
sql("array_agg IGNORE NULLS", "SELECT array_agg(x) IGNORE NULLS AS xs FROM VALUES (1), (NULL), (3) AS t(x)")
sql("max_by(x, y, k)", "SELECT max_by(product, revenue, 2) AS top2 FROM sales")


# 5. SQL PATH --------------------------------------------------------------------------------------
def sql_path():
    spark.conf.set("spark.sql.path.enabled", "true")
    spark.sql("SELECT current_path() AS path").show(truncate=False)


demo("current_path()", sql_path)


# 6. Arrow is the default for Python UDFs; zipWithIndex ------------------------------------------
@udf("string")
def shout(s):
    return s.upper() + "!"


demo("Python UDF (Arrow by default)", lambda: spark.table("sales").select(shout("product")).show())
demo("DataFrame.zipWithIndex", lambda: spark.table("sales").zipWithIndex().show())


# 7. Named streaming source -------------------------------------------------------------------------
def named_source():
    q = (spark.readStream.format("rate").option("rowsPerSecond", 5).name("clicks_src").load()
         .writeStream.format("memory").queryName("named_rate")
         .option("checkpointLocation", tempfile.mkdtemp()).start())
    q.awaitTermination(5)
    q.stop()
    print("rows:", spark.table("named_rate").count())


demo("DataStreamReader.name()", named_source)

# 8. Change Data Capture (needs a DSv2 table that implements Changelog) -------------------------------
#   spark.sql("SELECT * FROM cat.db.orders CHANGES FROM VERSION 10 TO VERSION 20").show()
#   spark.sql("SELECT * FROM STREAM cat.db.orders CHANGES FROM VERSION 0")      # streaming
#
# Real-Time Mode from Python (Kafka source/sink, update mode):
#   (df.writeStream.format("kafka")...outputMode("update").trigger(realTime="5 minutes").start())

spark.stop()
