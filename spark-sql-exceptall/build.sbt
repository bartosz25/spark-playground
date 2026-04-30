scalaVersion := "2.13.17"
name := "except_all"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "4.1.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-hive" % sparkVersion
