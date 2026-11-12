// Apache Spark 4.2.0 — feature tour (Scala)
// Covers: NEAREST BY + vector functions, QUALIFY, geospatial, Dataset.zipWithIndex, CDC (snippet).
import org.apache.spark.sql.SparkSession

object Spark420Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-4.2.0-features").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    demo("NEAREST BY (top-1 product per user by L2 distance)") {
      spark.sql(
        """WITH users AS (
          |  SELECT * FROM VALUES ('u1', CAST(array(0.1, 0.9) AS ARRAY<FLOAT>)),
          |                       ('u2', CAST(array(0.8, 0.2) AS ARRAY<FLOAT>)) AS t(user_id, emb)),
          |products AS (
          |  SELECT * FROM VALUES ('laptop', CAST(array(0.9, 0.1) AS ARRAY<FLOAT>)),
          |                       ('novel',  CAST(array(0.1, 0.8) AS ARRAY<FLOAT>)) AS t(product, emb))
          |SELECT u.user_id, p.product
          |FROM users u JOIN products p EXACT NEAREST 1 BY DISTANCE vector_l2_distance(u.emb, p.emb)
          |""".stripMargin).show()
    }

    demo("QUALIFY") {
      Seq(("a", 1), ("a", 5), ("b", 3), ("b", 2)).toDF("k", "v").createOrReplaceTempView("kv")
      spark.sql("SELECT k, v FROM kv QUALIFY rank() OVER (PARTITION BY k ORDER BY v DESC) = 1").show()
    }

    demo("Geospatial") {
      spark.sql("SELECT typeof(ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040'))) AS t").show()
    }

    demo("Dataset.zipWithIndex") {
      Seq("alpha", "beta", "gamma").toDF("name").zipWithIndex().show()
    }

    // CDC: SELECT * FROM cat.db.orders CHANGES FROM VERSION 10 TO VERSION 20
    spark.stop()
  }
}
