// Apache Spark 4.0.0 — feature tour (Java)
// Covers: ANSI default, VARIANT, collation, SQL UDF, pipe syntax.
import org.apache.spark.sql.SparkSession;

public class Spark400Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-4.0.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    demo("ANSI default", () -> {
      System.out.println("spark.sql.ansi.enabled = " + spark.conf().get("spark.sql.ansi.enabled"));
      spark.sql("SELECT try_divide(1, 0) AS safe_div, try_cast('x' AS INT) AS safe_cast").show();
    });

    sql(spark, "VARIANT",
        "SELECT variant_get(parse_json('{\"order\": {\"id\": 99, \"total\": 12.5}}'), '$.order.total', 'double') AS total");

    sql(spark, "Collation",
        "SELECT 'Straße' = 'STRASSE' COLLATE UNICODE_CI AS unicode_ci, 'Java' = 'JAVA' COLLATE UTF8_LCASE AS lcase");

    demo("SQL UDF", () -> {
      spark.sql("CREATE OR REPLACE TEMPORARY FUNCTION gst(amount DOUBLE) RETURNS DOUBLE RETURN amount * 0.18");
      spark.sql("SELECT amount, gst(amount) AS gst FROM VALUES (100.0), (250.0) AS t(amount)").show();
    });

    sql(spark, "Pipe syntax",
        "FROM VALUES ('a', 10), ('a', 20), ('b', 5) AS t(k, v) |> AGGREGATE sum(v) AS total GROUP BY k |> ORDER BY total DESC");

    spark.stop();
  }
}
