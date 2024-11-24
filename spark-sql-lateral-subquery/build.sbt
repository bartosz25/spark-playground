name := "lateral_subquery"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"
