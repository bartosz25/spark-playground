name := "event_time_skew"
organization := "com.waitingforcode"
version := "1.0.0-SNAPSHOT"


val sparkVersion = "3.5.1"

libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql-kafka-0-10" % sparkVersion
