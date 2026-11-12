// Apache Spark 3.3.0 — feature tour (Java)
// Covers: try_* functions, ILIKE, timestampadd/timestampdiff, Trigger.AvailableNow.
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;

public class Spark330Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-3.3.0-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    sql(spark, "try_* functions",
        "SELECT try_add(2147483647, 1) AS a, try_multiply(2147483647, 2) AS b, try_divide(10, 0) AS c");
    sql(spark, "ILIKE",
        "SELECT name FROM VALUES ('Apache Spark'), ('SPARK SQL'), ('Flink') AS t(name) WHERE name ILIKE '%spark%'");
    sql(spark, "timestampadd / timestampdiff",
        "SELECT timestampadd(MINUTE, 90, TIMESTAMP'2022-06-16 10:00:00') AS later, "
            + "timestampdiff(MONTH, DATE'2022-01-15', DATE'2022-06-16') AS months");

    demo("Trigger.AvailableNow", () -> {
      try {
        String src = "/tmp/spark330-java/in";
        spark.sql("SELECT id, id * 2 AS v FROM range(10)").write().mode("overwrite").json(src);
        StreamingQuery q = spark.readStream().schema("id long, v long").json(src)
            .writeStream().format("memory").queryName("available_now_java")
            .trigger(Trigger.AvailableNow()).start();
        q.awaitTermination();
        spark.table("available_now_java").show();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });

    spark.stop();
  }
}
