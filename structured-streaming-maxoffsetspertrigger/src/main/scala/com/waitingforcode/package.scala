package com

package object waitingforcode {

  val PerfectUseCaseTopicName = "perfect_use_case"
  val PerfectUseCaseCheckpoint = s"/tmp/waitingforcode/${PerfectUseCaseTopicName}/checkpoint"

  val LessPerfectUseCaseTopicName = "less_perfect_use_case"
  val LessPerfectUseCaseCheckpoint = s"/tmp/waitingforcode/${LessPerfectUseCaseTopicName}/checkpoint"

  val AnotherLessPerfectUseCaseTopicName = "another_less_perfect_use_case"
  val AnotherLessPerfectUseCaseCheckpoint = s"/tmp/waitingforcode/${AnotherLessPerfectUseCaseTopicName}/checkpoint"
}
