package com.amadeus.sparklear.collects

import com.amadeus.sparklear.input.SqlInput
import org.apache.spark.sql.execution.SparkPlanInfo

case class SqlCollect(id: Long, p: SparkPlanInfo) extends Collect[SqlInput]
