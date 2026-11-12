// Apache Spark 4.0.0 — feature tour (Scala)
// Covers: transformWithState (new arbitrary stateful operator), state data source reader,
//         VARIANT functions, collation, pipe syntax.
//
// transformWithState requires the RocksDB state store provider. The StatefulProcessor API below
// follows the 4.0 signatures; check the Structured Streaming guide if you are on a later 4.x.
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._

class RunningCount extends StatefulProcessor[String, (String, Long), (String, Long)] {
  @transient private var count: ValueState[Long] = _

  override def init(outputMode: OutputMode, timeMode: TimeMode): Unit = {
    count = getHandle.getValueState[Long]("count", Encoders.scalaLong, TTLConfig.NONE)
  }

  override def handleInputRows(
      key: String,
      inputRows: Iterator[(String, Long)],
      timerValues: TimerValues): Iterator[(String, Long)] = {
    val updated = count.getOption().getOrElse(0L) + inputRows.size
    count.update(updated)
    Iterator((key, updated))
  }
}

object Spark400Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("spark-4.0.0-features").master("local[*]")
      .config("spark.sql.streaming.stateStore.providerClass",
        "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val checkpoint = "/tmp/spark400/tws-ckpt"

    demo("transformWithState") {
      val events = spark.readStream.format("rate").option("rowsPerSecond", "20").load()
        .select(($"value" % 3).cast("string").as("key"), $"value")
        .as[(String, Long)]
      val q = events
        .groupByKey(_._1)
        .transformWithState(new RunningCount, TimeMode.None(), OutputMode.Update())
        .writeStream.format("memory").queryName("running_counts")
        .option("checkpointLocation", checkpoint)
        .outputMode("update").start()
      q.awaitTermination(10000)
      q.stop()
      spark.table("running_counts").orderBy($"_2".desc).show(6)
    }

    demo("State data source reader") {
      spark.read.format("statestore").load(checkpoint).show(5, truncate = false)
    }

    demo("VARIANT") {
      Seq("""{"device": {"os": "android", "version": 14}}""").toDF("json")
        .select(parse_json($"json").as("v"))
        .select(variant_get($"v", "$.device.os", "string").as("os"),
                variant_get($"v", "$.device.version", "int").as("version"))
        .show()
    }

    demo("Collation + pipe syntax") {
      Seq("Pune", "PUNE", "pune", "Delhi").toDF("city").createOrReplaceTempView("cities")
      spark.sql(
        """FROM cities
          ||> SELECT city COLLATE UTF8_LCASE AS city
          ||> AGGREGATE count(*) AS n GROUP BY city""".stripMargin).show()
    }

    spark.stop()
  }
}
