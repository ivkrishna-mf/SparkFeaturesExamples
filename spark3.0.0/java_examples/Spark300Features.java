// Apache Spark 3.0.0 — feature tour (Java)
// Covers: AQE, formatted explain, join hints, higher-order functions via SQL, new aggregates, ANSI mode.
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Spark300Features {

  static void demo(String title, Runnable body) {
    System.out.println("\n=== " + title + " ===");
    try {
      body.run();
    } catch (Exception e) {
      String msg = e.getMessage() == null ? "" : e.getMessage().split("\n")[0];
      System.out.println("[skipped] " + e.getClass().getSimpleName() + ": " + msg);
    }
  }

  static void sql(SparkSession spark, String title, String query) {
    demo(title, () -> spark.sql(query).show(false));
  }

  public static void main(String[] args) {
    SparkSession spark = SparkSession.builder()
        .appName("spark-3.0.0-features")
        .master("local[*]")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    Dataset<Row> orders = spark.range(0, 100000).select(
        col("id").as("order_id"), col("id").mod(500).as("cust_id"), rand(1).multiply(100).as("amount"));
    Dataset<Row> customers = spark.range(0, 500).select(
        col("id").as("cust_id"), when(col("id").mod(2).equalTo(0), "gold").otherwise("silver").as("tier"));
    orders.createOrReplaceTempView("orders");

    // 1. AQE + formatted explain
    demo("AQE + explain(\"formatted\")", () -> {
      Dataset<Row> revenue = orders.join(customers, "cust_id").groupBy("tier").agg(sum("amount").as("revenue"));
      revenue.explain("formatted");
      revenue.show();
    });

    // 2. New join hints
    demo("MERGE hint", () -> orders.join(customers.hint("merge"), "cust_id").explain());

    // 3. Higher-order functions (SQL lambdas work from any language)
    sql(spark, "transform / filter / aggregate",
        "SELECT transform(xs, x -> x * 10) AS times_ten, filter(xs, x -> x % 2 = 0) AS evens, "
            + "aggregate(xs, 0, (acc, x) -> acc + x) AS total FROM (SELECT array(1, 2, 3, 4) AS xs)");

    // 4. New aggregates
    sql(spark, "count_if / max_by / min_by",
        "SELECT count_if(amount > 50) AS big, max_by(order_id, amount) AS top, min_by(order_id, amount) AS low FROM orders");

    // 5. ANSI mode
    demo("ANSI mode overflow", () -> {
      spark.conf().set("spark.sql.ansi.enabled", "true");
      try {
        spark.sql("SELECT 2147483647 + 1").collect();
      } catch (Exception e) {
        System.out.println("ANSI mode raised " + e.getClass().getSimpleName());
      } finally {
        spark.conf().set("spark.sql.ansi.enabled", "false");
      }
    });

    spark.stop();
  }
}
