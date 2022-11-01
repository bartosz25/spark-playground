package com.waitingforcode

object WrappedIterators extends App {

  class ExecutionTimeIterator[A](parentIterator: Iterator[A]) extends Iterator[A] {
    var firstNextCall: Option[Long] = None
    var lastNextCall: Option[Long] = None

    // firstNextCall.get - if we have the last, we also have the first
    def executionTime = lastNextCall.map(lastCall => lastCall - firstNextCall.get)

    override def hasNext: Boolean = parentIterator.hasNext
    override def next(): A = {
      if (firstNextCall.isEmpty) {
        firstNextCall = Some(System.currentTimeMillis())
      }
      val nextElement = parentIterator.next()
      if (!hasNext) {
        lastNextCall = Some(System.currentTimeMillis())
      }
      nextElement
    }
  }
  val numbers = new ExecutionTimeIterator(Iterator(1, 2, 3))
  val mappedNumbersFromWrapper = numbers.map(number => number).toSeq
  assert(mappedNumbersFromWrapper == Seq(1, 2, 3))
  assert(numbers.executionTime.get > 0)
  println(s"executionTime=${numbers.executionTime.get}")
}
