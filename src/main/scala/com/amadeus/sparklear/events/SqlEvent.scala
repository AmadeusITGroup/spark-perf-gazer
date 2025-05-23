package com.amadeus.sparklear.events

import com.amadeus.sparklear.entities.SqlEntity
import org.apache.spark.sql.execution.SparkPlanInfo

/**
  * Raw event proving information about a SQL query
  */
case class SqlEvent(
  id: Long,
  plan: SparkPlanInfo,
  description: String
) extends Event[SqlEntity]
