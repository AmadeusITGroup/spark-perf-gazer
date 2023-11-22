package com.amadeus.sparklear.wrappers

import com.amadeus.sparklear.reports.SqlReport
import org.apache.spark.sql.execution.SparkPlanInfo

case class SqlWrapper(id: Long, p: SparkPlanInfo) extends Wrapper[SqlReport] {
  override def toReport(): SqlReport = ???
}
