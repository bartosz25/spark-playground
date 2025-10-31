scalaVersion := "2.13.16"
name := "spark_show"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "4.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
