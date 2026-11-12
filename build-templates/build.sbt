// Generic sbt build for any release folder's scala_examples.
//   sbt -Dspark.version=4.3.0 -Dexamples.dir=../spark4.3.0/scala_examples package
val sparkVersion = sys.props.getOrElse("spark.version", "4.3.0")
val examplesDir  = sys.props.getOrElse("examples.dir", "../spark4.3.0/scala_examples")

name         := "spark-release-examples"
version      := "0.1"
scalaVersion := {
  val v = sparkVersion
  if (v.startsWith("3.")) "2.12.18"
  else if (v.startsWith("4.0")) "2.13.16"
  else if (v.startsWith("4.1")) "2.13.17"
  else "2.13.18"
}

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion % "provided" // only for the RTM sketches
)

Compile / unmanagedSourceDirectories := Seq(baseDirectory.value / examplesDir)
