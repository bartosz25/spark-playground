scalaVersion := "2.13.16"
name := "new_state_api"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "4.0.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion