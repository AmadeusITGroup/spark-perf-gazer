package org.apache.spark.sql.execution.ui // ATTENTION: spark package to access to the event.qe.executedPlan

import org.apache.spark.sql.execution._

object SparkInternal {
  def executedPlan(event: SparkListenerSQLExecutionEnd): SparkPlan = {
    event.qe.executedPlan
  }

  def queryExecution(event: SparkListenerSQLExecutionEnd): QueryExecution = {
    event.qe
  }
}
