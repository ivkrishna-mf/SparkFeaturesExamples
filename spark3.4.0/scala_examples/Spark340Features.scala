// Apache Spark 3.4.0 — feature tour (Scala)
// Covers: Dataset.unpivot, Dataset.to(schema), withColumnsRenamed, lateral column alias,
//         TIMESTAMP_NTZ, DEFAULT column values.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

object Spark340Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.4.0-features").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val wide = Seq((1, 100, 120), (2, 80, 95)).toDF("store", "q1", "q2")

    demo("unpivot") {
      wide.unpivot(Array($"store"), Array($"q1", $"q2"), "quarter", "revenue").show()
    }

    demo("Dataset.to(schema) — reorder + up-cast") {
      val target = StructType(Seq(StructField("q1", LongType), StructField("store", IntegerType)))
      wide.to(target).printSchema()
    }

    demo("withColumnsRenamed") {
      wide.withColumnsRenamed(Map("q1" -> "quarter_1", "q2" -> "quarter_2")).show()
    }

    demo("Lateral column alias + TIMESTAMP_NTZ") {
      spark.sql("SELECT 100 AS price, price * 0.18 AS gst, price + gst AS total, TIMESTAMP_NTZ'2023-04-13 09:00:00' AS ntz").show()
    }

    demo("DEFAULT column values") {
      spark.sql("DROP TABLE IF EXISTS tickets")
      spark.sql("CREATE TABLE tickets (id INT, priority STRING DEFAULT 'P3') USING parquet")
      spark.sql("INSERT INTO tickets (id) VALUES (1)")
      spark.table("tickets").show()
    }

    spark.stop()
  }
}
