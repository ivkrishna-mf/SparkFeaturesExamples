// Apache Spark 4.3.0 — feature tour (Scala)
// Covers: ASOF JOIN, JSON_TABLE, UNNEST, VARIANT editing, stateful Real-Time Mode (sketch).
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark430Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  /**
   * Stateful Real-Time Mode (4.3): a streaming aggregation in RTM. Needs Kafka, so main does not call it.
   * Verify the trigger import against the RTM section of the Structured Streaming guide for your build.
   */
  def realTimeStatefulSketch(spark: SparkSession, brokers: String): Unit = {
    import org.apache.spark.sql.execution.streaming.RealTimeTrigger
    import spark.implicits._
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", brokers).option("subscribe", "card_swipes").load()
      .select($"key".cast("string").as("card"))
      .groupBy("card").count()                         // stateful aggregation, now allowed in RTM
      .selectExpr("card AS key", "CAST(count AS STRING) AS value")
      .writeStream.format("kafka")
      .option("kafka.bootstrap.servers", brokers).option("topic", "swipe_counts")
      .option("checkpointLocation", "/tmp/spark430/rtm-ckpt")
      .outputMode("update")
      .trigger(RealTimeTrigger.apply("5 minutes"))
      .start()
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-4.3.0-features").master("local[*]")
      .config("spark.sql.join.asofJoin.enabled", "true")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    demo("ASOF JOIN") {
      Seq(("AAPL", 1, 190.0), ("AAPL", 5, 191.5), ("MSFT", 2, 410.0)).toDF("sym", "ts", "price")
        .createOrReplaceTempView("quotes")
      Seq(("AAPL", 3, 10), ("AAPL", 6, 5), ("MSFT", 4, 7)).toDF("sym", "ts", "qty")
        .createOrReplaceTempView("trades")
      spark.sql(
        """SELECT t.sym, t.ts, t.qty, q.price AS price_at_trade
          |FROM trades t ASOF JOIN quotes q
          |  MATCH_CONDITION (t.ts >= q.ts)
          |  ON t.sym = q.sym
          |ORDER BY t.sym, t.ts""".stripMargin).show()
    }

    demo("JSON_TABLE") {
      spark.sql(
        """SELECT * FROM JSON_TABLE(
          |  '[{"city":"Pune","temp":31},{"city":"Oslo","temp":4}]', '$[*]'
          |  COLUMNS (city STRING, temp INT))""".stripMargin).show()
    }

    demo("UNNEST WITH ORDINALITY") {
      spark.sql("SELECT * FROM UNNEST(array(10, 20, 30)) WITH ORDINALITY AS t(v, pos)").show()
    }

    demo("VARIANT editing") {
      spark.sql(
        """SELECT to_json(variant_insert(parse_json('{"a":1}'), '$.b', 2)) AS inserted,
          |       to_json(variant_delete(parse_json('{"a":1,"b":2}'), '$.a')) AS deleted""".stripMargin).show(false)
    }

    spark.stop()
  }
}
