name := "sparkTest"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.7"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.1.0"

resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.github.shin285" %% "KOMORAN" % "3.3.4"
