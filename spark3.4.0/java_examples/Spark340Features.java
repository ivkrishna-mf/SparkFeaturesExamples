// Apache Spark 3.4.0 — feature tour (Java)
// Covers: unpivot, offset, lateral column alias, DEFAULT column values.
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;

public class Spark340Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-3.4.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    Dataset<Row> wide = spark.sql("SELECT * FROM VALUES (1, 100, 120), (2, 80, 95) AS t(store, q1, q2)");

    demo("unpivot", () -> wide.unpivot(
        new Column[] {col("store")}, new Column[] {col("q1"), col("q2")}, "quarter", "revenue").show());
    demo("offset", () -> spark.range(10).offset(7).show());
    sql(spark, "Lateral column alias", "SELECT 250.0 AS price, price * 0.18 AS gst, price + gst AS total");

    demo("DEFAULT column values", () -> {
      spark.sql("DROP TABLE IF EXISTS java_tickets");
      spark.sql("CREATE TABLE java_tickets (id INT, priority STRING DEFAULT 'P3') USING parquet");
      spark.sql("INSERT INTO java_tickets VALUES (1, DEFAULT)");
      spark.table("java_tickets").show();
    });

    spark.stop();
  }
}
