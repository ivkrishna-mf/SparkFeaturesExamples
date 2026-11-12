// Apache Spark 4.2.0 — feature tour (Java)
// Covers: QUALIFY, NEAREST BY + vector_cosine_similarity, geospatial.
import org.apache.spark.sql.SparkSession;

public class Spark420Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-4.2.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    sql(spark, "QUALIFY",
        "SELECT dept, name, salary FROM VALUES ('eng', 'a', 100), ('eng', 'b', 120), ('ops', 'c', 90) AS t(dept, name, salary) "
            + "QUALIFY row_number() OVER (PARTITION BY dept ORDER BY salary DESC) = 1");

    sql(spark, "NEAREST BY",
        "SELECT q.id, d.title FROM "
            + "(SELECT 1 AS id, CAST(array(1.0, 0.0) AS ARRAY<FLOAT>) AS emb) q "
            + "JOIN (SELECT * FROM VALUES ('east', CAST(array(1.0, 0.1) AS ARRAY<FLOAT>)), "
            + "('north', CAST(array(0.0, 1.0) AS ARRAY<FLOAT>)) AS t(title, emb)) d "
            + "EXACT NEAREST 1 BY SIMILARITY vector_cosine_similarity(q.emb, d.emb)");

    sql(spark, "Geospatial",
        "SELECT hex(ST_AsBinary(ST_GeomFromWKB(unhex('0101000000000000000000F03F0000000000000040')))) AS wkb");

    spark.stop();
  }
}
