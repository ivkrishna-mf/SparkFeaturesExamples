// Apache Spark 3.2.0 — feature tour (Scala)
// Covers: session_window, ANSI intervals (make_ym_interval / make_dt_interval), RocksDB state store.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark320Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-3.2.0-features").master("local[*]")
      .config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    demo("session_window") {
      Seq(("u1", "2021-10-13 10:00:00"), ("u1", "2021-10-13 10:03:00"), ("u1", "2021-10-13 10:20:00"))
        .toDF("user", "ts").withColumn("ts", to_timestamp($"ts"))
        .groupBy($"user", session_window($"ts", "5 minutes")).count()
        .show(false)
    }

    demo("ANSI intervals") {
      spark.sql(
        """SELECT make_ym_interval(1, 6)        AS ym,
          |       make_dt_interval(2, 3, 4, 5) AS dt,
          |       TIMESTAMP'2021-10-13 12:00:00' - TIMESTAMP'2021-10-12 08:30:00' AS elapsed""".stripMargin).show(false)
    }

    demo("RocksDB-backed streaming dedup") {
      val q = spark.readStream.format("rate").option("rowsPerSecond", "50").load()
        .withColumn("key", $"value" % 10)
        .dropDuplicates("key")
        .writeStream.format("memory").queryName("unique_keys").outputMode("append").start()
      q.awaitTermination(8000)
      q.stop()
      spark.table("unique_keys").orderBy("key").show()
    }

    spark.stop()
  }
}
