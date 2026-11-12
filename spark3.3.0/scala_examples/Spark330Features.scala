// Apache Spark 3.3.0 — feature tour (Scala)
// Covers: Trigger.AvailableNow, try_* functions, Column.ilike, runtime Bloom filter config.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object Spark330Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.3.0-features").master("local[*]")
      .config("spark.sql.optimizer.runtime.bloomFilter.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    demo("try_* functions") {
      spark.sql("SELECT try_add(2147483647, 1) AS a, try_divide(1, 0) AS b, try_element_at(array(1), 5) AS c").show()
    }

    demo("Column.ilike") {
      Seq("Apache Spark", "SPARK SQL", "Flink").toDF("name").filter($"name".ilike("%spark%")).show()
    }

    demo("Trigger.AvailableNow") {
      val src = "/tmp/spark330/in"
      Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "v").write.mode("overwrite").json(src)
      val q = spark.readStream.schema("id long, v string").json(src)
        .writeStream.format("memory").queryName("available_now")
        .trigger(Trigger.AvailableNow())
        .start()
      q.awaitTermination()
      spark.table("available_now").show()
    }

    spark.stop()
  }
}
