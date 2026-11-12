// Apache Spark 4.1.0 — feature tour (Scala)
// Covers: SQL scripting GA, recursive CTE, VARIANT colon syntax, Real-Time Mode (sketch).
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Spark410Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  /**
   * Real-Time Mode (4.1: Scala, stateless, update output mode). Needs a Kafka source and sink, so it is
   * not called from main. Verify the trigger import against the Structured Streaming RTM guide for your build.
   */
  def realTimeModeSketch(spark: SparkSession, brokers: String): Unit = {
    import org.apache.spark.sql.execution.streaming.RealTimeTrigger
    val in: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", brokers).option("subscribe", "payments").load()
    in.selectExpr("CAST(key AS STRING) AS key", "upper(CAST(value AS STRING)) AS value")
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", brokers).option("topic", "payments_clean")
      .option("checkpointLocation", "/tmp/spark410/rtm-ckpt")
      .outputMode("update")
      .trigger(RealTimeTrigger.apply("5 minutes"))   // long-running batch; records flow continuously
      .start()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-4.1.0-features").master("local[*]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    demo("SQL scripting with CONTINUE HANDLER") {
      spark.sql(
        """BEGIN
          |  DECLARE errors INT DEFAULT 0;
          |  DECLARE CONTINUE HANDLER FOR SQLEXCEPTION SET errors = errors + 1;
          |  SELECT 1 / 0;              -- raises DIVIDE_BY_ZERO, handled, script continues
          |  SELECT errors AS handled_errors;
          |END""".stripMargin).show()
    }

    demo("Recursive CTE: Fibonacci") {
      spark.sql(
        """WITH RECURSIVE fib(n, a, b) AS (
          |  SELECT 1, 0L, 1L
          |  UNION ALL
          |  SELECT n + 1, b, a + b FROM fib WHERE n < 15
          |)
          |SELECT n, a AS fib FROM fib ORDER BY n""".stripMargin).show(15)
    }

    demo("VARIANT colon syntax") {
      spark.sql("""SELECT v:device.os::string AS os FROM (SELECT parse_json('{"device":{"os":"linux"}}') AS v)""").show()
    }

    spark.stop()
  }
}
