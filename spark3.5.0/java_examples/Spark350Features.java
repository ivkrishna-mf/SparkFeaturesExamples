// Apache Spark 3.5.0 — feature tour (Java)
// Covers: parameterized SQL (java.util.Map + Object[]), HLL sketches via SQL.
import java.util.Map;

import org.apache.spark.sql.SparkSession;

public class Spark350Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-3.5.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    demo("Named parameters", () -> spark.sql(
        "SELECT id FROM range(10) WHERE id BETWEEN :lo AND :hi",
        Map.<String, Object>of("lo", 3, "hi", 5)).show());

    demo("Positional parameters", () -> spark.sql(
        "SELECT id FROM range(10) WHERE id > ?", new Object[] {7}).show());

    sql(spark, "HLL sketches",
        "SELECT hll_sketch_estimate(hll_sketch_agg(u)) AS approx_distinct "
            + "FROM VALUES ('a'), ('b'), ('b'), ('c') AS t(u)");

    spark.stop();
  }
}
