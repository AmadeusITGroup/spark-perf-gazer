package com.amadeus.perfgazer.events

import org.apache.spark.sql.execution.SparkPlanInfo

/**
  * Raw event proving information about a SQL query
  */
case class SqlEvent(
  id: Long,
  description: String,
  details: String,
  planInfo: SparkPlanInfo
)
