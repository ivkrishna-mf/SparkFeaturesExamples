// Apache Spark 4.1.0 — feature tour (Java)
// Covers: SQL scripting GA, recursive CTE, VARIANT colon syntax, approx_top_k.
import org.apache.spark.sql.SparkSession;

public class Spark410Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-4.1.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    sql(spark, "SQL scripting",
        "BEGIN DECLARE x, y INT DEFAULT 3; SET y = x * 7; SELECT x, y; END");

    sql(spark, "Recursive CTE",
        "WITH RECURSIVE r(n) AS (SELECT 1 UNION ALL SELECT n * 2 FROM r WHERE n < 512) SELECT collect_list(n) AS powers_of_two FROM r");

    sql(spark, "VARIANT colon syntax",
        "SELECT v:order.id::int AS id, v:order.total::double AS total "
            + "FROM (SELECT parse_json('{\"order\":{\"id\":5,\"total\":19.9}}') AS v)");

    sql(spark, "approx_top_k",
        "SELECT approx_top_k(lang, 2) AS top FROM VALUES ('java'), ('scala'), ('java'), ('python'), ('java'), ('python') AS t(lang)");

    spark.stop();
  }
}
