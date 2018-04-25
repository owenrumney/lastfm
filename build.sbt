name := "lastfm"

version := "0.1"

scalaVersion := "2.11.11"

val sparkVersion = "2.3.0"
val scalaTestVersion = "3.0.5"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-core_2.11" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % Test
)
