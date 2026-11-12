# Build templates

One sbt build and one Maven build serve every release folder — pass the Spark version and the folder.

| Spark | Scala binary | Java (`-Djava.release`) |
|---|---|---|
| 3.0 – 3.2 | 2.12 | 8 or 11 |
| 3.3 – 3.5 | 2.12 (2.13 also published) | 11 or 17 |
| 4.0 – 4.3 | 2.13 | 17+ |

```bash
# Scala
sbt -Dspark.version=3.5.0 -Dexamples.dir=../spark3.5.0/scala_examples package
spark-submit --class Spark350Features target/scala-2.12/spark-release-examples_2.12-0.1.jar

# Java
mvn -q package -Dspark.version=3.5.0 -Dscala.binary.version=2.12 -Djava.release=11 \
    -Dexamples.dir=../spark3.5.0/java_examples
spark-submit --class Spark350Features target/spark-release-examples-0.1.jar
```

Spark is a `provided` dependency, so run the jars with `spark-submit` from a Spark distribution of the same version.
The Kafka connector is only needed for the Real-Time Mode sketches in 4.1 / 4.3 (add
`--packages org.apache.spark:spark-sql-kafka-0-10_2.13:<version>`).
