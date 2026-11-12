// Apache Spark 3.0.0 — feature tour (Scala)
// Covers: Dynamic Partition Pruning, AQE, join hints, Scala higher-order functions,
//         count_if / max_by / min_by, make_date / make_timestamp.
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark300Features {

  def demo(title: String)(body: => Unit): Unit = {
    println(s"\n=== $title ===")
    try body catch {
      case e: Exception =>
        val msg = Option(e.getMessage).map(_.split("\n").head).getOrElse("")
        println(s"[skipped] ${e.getClass.getSimpleName}: $msg")
    }
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("spark-3.0.0-features")
      .master("local[*]")
      .config("spark.sql.adaptive.enabled", "true")   // AQE: off by default in 3.0
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 1. Dynamic Partition Pruning: partitioned fact table joined to a filtered dimension
    val factPath = "/tmp/spark300/sales"
    spark.range(0, 100000)
      .select($"id".as("sale_id"), ($"id" % 30).cast("int").as("day"), (rand(7) * 100).as("amount"))
      .write.mode("overwrite").partitionBy("day").parquet(factPath)
    val sales = spark.read.parquet(factPath)
    val days = (0 until 30).map(d => (d, if (d % 7 == 0) "sunday" else "weekday")).toDF("day", "kind")

    demo("Dynamic Partition Pruning") {
      val sundaySales = sales.join(days.filter($"kind" === "sunday"), "day")
      sundaySales.explain("formatted") // the parquet scan shows a dynamicpruningexpression(...)
      sundaySales.groupBy("day").agg(sum("amount").as("revenue")).orderBy("day").show()
    }

    // 2. New join hints
    demo("SHUFFLE_REPLICATE_NL / MERGE hints") {
      sales.join(days.hint("merge"), "day").explain()
    }

    // 3. Higher-order functions are now first-class in the Scala API
    demo("transform / filter / exists / aggregate") {
      Seq((1, Seq(1, 2, 3, 4)), (2, Seq(5, 6))).toDF("id", "xs").select(
        $"id",
        transform($"xs", x => x * 10).as("times_ten"),
        filter($"xs", x => x % 2 === 0).as("evens"),
        exists($"xs", x => x > 5).as("has_gt_5"),
        aggregate($"xs", lit(0), (acc, x) => acc + x).as("total")
      ).show()
    }

    // 4. New aggregate & constructor functions
    sales.createOrReplaceTempView("sales")
    demo("count_if / max_by / min_by") {
      spark.sql(
        """SELECT count_if(amount > 50)   AS big_sales,
          |       max_by(sale_id, amount) AS top_sale,
          |       min_by(sale_id, amount) AS smallest_sale
          |FROM sales""".stripMargin).show()
    }
    demo("make_date / make_timestamp") {
      spark.sql("SELECT make_date(2020, 6, 18) AS d, make_timestamp(2020, 6, 18, 9, 30, 0) AS ts").show(false)
    }

    spark.stop()
  }
}
