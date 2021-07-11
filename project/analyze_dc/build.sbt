name := "analyze_dc"

version := "0.1"

scalaVersion := "2.12.0"

val sparkVersion = "2.4.8"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "com.github.shin285" % "KOMORAN" % "3.3.4"
)