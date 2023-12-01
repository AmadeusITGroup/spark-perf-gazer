package com.amadeus.sparklear.wrappers

import com.amadeus.sparklear.input.SqlInput
import org.apache.spark.sql.execution.SparkPlanInfo

case class SqlWrapper(id: Long, p: SparkPlanInfo) extends Wrapper[SqlInput] {
  override def toReport(): SqlInput = ???
}
