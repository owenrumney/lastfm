name := "lastfm"

version := "0.1"

scalaVersion := "2.11.11"

val databricksVersion = "1.5.0"
val sparkVersion = "2.3.0"
// test
val scalaTestVersion = "3.0.5"
val scalaMockVersion = "4.1.0"

libraryDependencies ++= Seq(
  "com.databricks" %% "spark-csv" % databricksVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  // test
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test,
  "org.scalamock" %% "scalamock" % scalaMockVersion % Test
)
