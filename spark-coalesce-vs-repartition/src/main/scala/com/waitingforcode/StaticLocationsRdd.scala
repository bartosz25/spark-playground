package com.waitingforcode

import org.apache.spark.rdd.RDD
import org.apache.spark.{Dependency, Partition, SparkContext, TaskContext}

class StaticLocationsRdd(_sc: SparkContext, deps: Seq[Dependency[_]]) extends RDD[String](_sc, deps) {
  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    Iterator(s"${split} a", s"${split} b")
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    if (split.index < 4) {
      Seq("host1")
    } else if (split.index < 8) {
      Seq("host2")
    } else {
      Seq("host3")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    (0 to 11).map(nr => StaticLocationPartition(nr)).toArray
  }
}
case class StaticLocationPartition(override val index: Int) extends Partition
