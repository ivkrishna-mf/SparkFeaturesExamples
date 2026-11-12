// Apache Spark 3.2.0 — feature tour (Java)
// Covers: AQE default-on, session_window, ANSI intervals, LATERAL subquery.
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class Spark320Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-3.2.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    demo("AQE default", () ->
        System.out.println("spark.sql.adaptive.enabled = " + spark.conf().get("spark.sql.adaptive.enabled")));

    demo("session_window", () -> {
      Dataset<Row> clicks = spark.sql(
          "SELECT user, to_timestamp(ts) AS ts FROM VALUES "
              + "('u1', '2021-10-13 10:00:00'), ('u1', '2021-10-13 10:03:00'), ('u1', '2021-10-13 10:20:00') AS t(user, ts)");
      clicks.groupBy(col("user"), session_window(col("ts"), "5 minutes")).count().show(false);
    });

    sql(spark, "ANSI intervals",
        "SELECT INTERVAL '1-6' YEAR TO MONTH AS ym, DATE'2021-10-13' + INTERVAL '1-6' YEAR TO MONTH AS later");

    sql(spark, "LATERAL subquery",
        "SELECT c.cust, t.max_amount FROM VALUES ('c1'), ('c2') AS c(cust), "
            + "LATERAL (SELECT max(amount) AS max_amount FROM VALUES ('c1', 10), ('c1', 40), ('c2', 25) AS o(cust, amount) "
            + "WHERE o.cust = c.cust) t");

    spark.stop();
  }
}
