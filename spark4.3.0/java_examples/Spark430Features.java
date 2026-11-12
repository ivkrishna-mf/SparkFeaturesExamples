// Apache Spark 4.3.0 — feature tour (Java)
// Covers: ASOF JOIN, JSON_TABLE, UNNEST WITH ORDINALITY, new string functions.
import org.apache.spark.sql.SparkSession;

public class Spark430Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-4.3.0-features").master("local[*]")
        .config("spark.sql.join.asofJoin.enabled", "true")
        .getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    sql(spark, "ASOF JOIN",
        "SELECT s.sensor, s.ts, c.factor FROM "
            + "(SELECT * FROM VALUES ('s1', 5), ('s1', 12) AS t(sensor, ts)) s "
            + "LEFT ASOF JOIN (SELECT * FROM VALUES ('s1', 0, 1.0), ('s1', 10, 1.1) AS t(sensor, ts, factor)) c "
            + "MATCH_CONDITION (s.ts >= c.ts) ON s.sensor = c.sensor ORDER BY s.ts");

    sql(spark, "JSON_TABLE",
        "SELECT * FROM JSON_TABLE('{\"tags\":[{\"k\":\"env\",\"v\":\"prod\"},{\"k\":\"team\",\"v\":\"data\"}]}', "
            + "'$.tags[*]' COLUMNS (k STRING, v STRING))");

    sql(spark, "UNNEST WITH ORDINALITY",
        "SELECT * FROM UNNEST(array('x', 'y', 'z')) WITH ORDINALITY AS t(letter, pos)");

    sql(spark, "jaro_winkler_similarity",
        "SELECT jaro_winkler_similarity('dixon', 'dicksonx') AS jw");

    spark.stop();
  }
}
