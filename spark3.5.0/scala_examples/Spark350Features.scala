// Apache Spark 3.5.0 — feature tour (Scala)
// Covers: parameterized SQL (named Map + positional Array), IDENTIFIER clause,
//         HLL sketch functions, dropDuplicatesWithinWatermark.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark350Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.5.0-features").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    demo("Named parameters") {
      spark.sql("SELECT id FROM range(10) WHERE id BETWEEN :lo AND :hi", Map("lo" -> 3, "hi" -> 5)).show()
    }
    demo("Positional parameters") {
      spark.sql("SELECT id FROM range(10) WHERE id > ?", Array(7)).show()
    }
    demo("IDENTIFIER clause") {
      spark.range(3).createOrReplaceTempView("scala_tbl")
      spark.sql("SELECT count(*) FROM IDENTIFIER(:t)", Map("t" -> "scala_tbl")).show()
    }

    demo("HLL sketch functions") {
      Seq((1, "a"), (1, "b"), (2, "b"), (2, "c")).toDF("day", "user")
        .groupBy("day").agg(hll_sketch_agg($"user").as("sketch"))
        .agg(hll_sketch_estimate(hll_union_agg($"sketch")).as("distinct_users"))
        .show()
    }

    demo("dropDuplicatesWithinWatermark") {
      val q = spark.readStream.format("rate").option("rowsPerSecond", "20").load()
        .withColumn("event_id", $"value" % 7)
        .withWatermark("timestamp", "10 seconds")
        .dropDuplicatesWithinWatermark("event_id")
        .writeStream.format("memory").queryName("dedup_scala").start()
      q.awaitTermination(8000)
      q.stop()
      spark.table("dedup_scala").orderBy("event_id").show()
    }

    spark.stop()
  }
}
