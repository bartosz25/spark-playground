package com.waitingforcode

case class User(id: Option[String], login: String, number: Option[Int])
case class RegisteredUser(userId: Option[String], registerDate: Int, registeredNumber: Option[Int])