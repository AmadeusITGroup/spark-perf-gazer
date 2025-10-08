package com.amadeus.sparklear.entities

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.events.SqlEvent
import org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionEnd

@Unstable
case class SqlEntity(
  start: SqlEvent,
  end: SparkListenerSQLExecutionEnd
) extends Entity
