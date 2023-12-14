package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.SqlCollect
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

@Unstable
case class SqlPreReport(
  collect: SqlCollect,
  end: SparkListenerSQLExecutionEnd
) extends PreReport
