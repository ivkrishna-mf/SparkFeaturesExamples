// Apache Spark 3.1 — feature tour (Java)
// Covers: unionByName(allowMissingColumns), withField/dropFields, nth_value, regexp_extract_all,
//         timestamp_seconds, current_timezone.
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;

import static org.apache.spark.sql.functions.*;

public class Spark310Features {

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
    SparkSession spark = SparkSession.builder().appName("spark-3.1-features").master("local[*]").getOrCreate();
    spark.sparkContext().setLogLevel("WARN");

    demo("unionByName(..., allowMissingColumns=true)", () -> {
      Dataset<Row> jan = spark.sql("SELECT 1 AS id, 'alice' AS name, 10.0 AS amount");
      Dataset<Row> feb = spark.sql("SELECT 2 AS id, 'bob' AS name, 'EU' AS region");
      jan.unionByName(feb, true).show();
    });

    demo("withField / dropFields", () -> {
      Dataset<Row> df = spark.sql("SELECT named_struct('street', 'Main St', 'city', 'Pune', 'zip', '411001') AS address");
      df.select(
          col("address").withField("country", lit("IN")).as("with_country"),
          col("address").dropFields("zip").as("without_zip")).show(false);
    });

    demo("nth_value", () -> {
      Dataset<Row> sales = spark.sql(
          "SELECT * FROM VALUES ('north', 1, 100), ('north', 2, 300), ('south', 1, 50), ('south', 2, 80) AS t(region, month, revenue)");
      WindowSpec w = Window.partitionBy("region").orderBy("month")
          .rowsBetween(Window.unboundedPreceding(), Window.unboundedFollowing());
      sales.withColumn("month2_revenue", nth_value(col("revenue"), 2).over(w)).show();
    });

    sql(spark, "regexp_extract_all / timestamp_seconds / current_timezone",
        "SELECT regexp_extract_all('order-12, order-34', 'order-(\\\\d+)', 1) AS ids, "
            + "timestamp_seconds(1609459200) AS ts, current_timezone() AS tz");

    spark.stop();
  }
}
