package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.raw.SqlRawEvent
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

@Unstable
case class SqlPreReport(
  raw: SqlRawEvent,
  end: SparkListenerSQLExecutionEnd
) extends PreReport
