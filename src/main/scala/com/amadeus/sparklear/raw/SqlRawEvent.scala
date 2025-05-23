package com.amadeus.sparklear.raw

import com.amadeus.sparklear.prereports.SqlPreReport
import org.apache.spark.sql.execution.SparkPlanInfo

/**
  * Raw event proving information about a SQL query
  */
case class SqlRawEvent(
  id: Long,
  plan: SparkPlanInfo,
  description: String
) extends RawEvent[SqlPreReport]
