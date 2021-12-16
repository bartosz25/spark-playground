package com.waitingforcode

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class UserDefinedTypeTest extends AnyFlatSpec with Matchers  {

  // You can run the code with Spark 3.1.0 to see the registering problem
  it should "registre the UDT in Apache Spark 3.2.0" in {
    val sparkSession = SparkSession.builder()
      .appName("SPARK-7768 : promote UDT to the public API").master("local[*]")
      .getOrCreate()

    UDTRegistration.register("com.waitingforcode.Account",
      "com.waitingforcode.AccountUserDefinedType")
    import sparkSession.implicits._

    val schema = Seq(Account("abc", 1), Account("def", 2)).toDF("account").schema

    schema.simpleString shouldEqual "struct<account:accountuserdefinedtype_typename_override>"
  }

}

case class Account(nr: String, priority: Int)

class AccountUserDefinedType extends UserDefinedType[Account] {
  override def sqlType: DataType = StructType(Seq(
    StructField("nr", DataTypes.StringType, nullable = false),
    StructField("priority", DataTypes.IntegerType, nullable = false)
  ))

  override def serialize(accountClass: Account): InternalRow = {
    InternalRow(UTF8String.fromString(accountClass.nr), accountClass.priority)
  }

  override def deserialize(datum: Any): Account = datum match {
    case row: InternalRow => Account(row.getString(0), row.getInt(1))
  }

  override def userClass: Class[Account] = classOf[Account]

  override def typeName: String = "accountuserdefinedtype_typename_override"
}