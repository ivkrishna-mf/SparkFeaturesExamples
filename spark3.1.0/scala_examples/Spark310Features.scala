// Apache Spark 3.1 — feature tour (Scala)
// Covers: unionByName(allowMissingColumns), withField/dropFields (nested), nth_value,
//         percentile_approx, timestamp_seconds, streaming toTable().
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object Spark310Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.1-features").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    demo("unionByName(allowMissingColumns = true)") {
      val jan = Seq((1, "alice", 10.0)).toDF("id", "name", "amount")
      val feb = Seq((2, "bob", "EU")).toDF("id", "name", "region")
      jan.unionByName(feb, allowMissingColumns = true).show()
    }

    demo("withField on a nested path / dropFields") {
      val df = spark.sql("SELECT named_struct('name', 'ann', 'address', named_struct('city', 'Pune', 'zip', '411001')) AS person")
      df.select(
        $"person".withField("address.country", lit("IN")).as("with_country"),
        $"person".dropFields("address.zip").as("without_zip")
      ).show(false)
    }

    val sales = Seq(("north", 1, 100), ("north", 2, 300), ("north", 3, 200), ("south", 1, 50), ("south", 2, 80))
      .toDF("region", "month", "revenue")

    demo("nth_value + percentile_approx") {
      val w = Window.partitionBy("region").orderBy("month")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
      sales.withColumn("month2_revenue", nth_value($"revenue", 2).over(w)).show()
      sales.groupBy("region").agg(percentile_approx($"revenue", lit(0.5), lit(10000)).as("median")).show()
    }

    demo("timestamp_seconds") {
      Seq(1609459200L).toDF("epoch").select($"epoch", timestamp_seconds($"epoch").as("ts")).show(false)
    }

    demo("Streaming toTable()") {
      spark.sql("DROP TABLE IF EXISTS rate_events_scala")
      val q = spark.readStream.format("rate").option("rowsPerSecond", "20").load()
        .writeStream.option("checkpointLocation", "/tmp/spark310/ckpt")
        .toTable("rate_events_scala")
      q.awaitTermination(8000)
      q.stop()
      spark.table("rate_events_scala").orderBy("value").show(5)
    }

    spark.stop()
  }
}
