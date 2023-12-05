package com.amadeus.sparklear.collects

import com.amadeus.sparklear.prereports.SqlPreReport
import org.apache.spark.sql.execution.SparkPlanInfo

case class SqlCollect(
  id: Long,
  p: SparkPlanInfo,
  description: String
) extends Collect[SqlPreReport]
