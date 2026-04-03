package org.apache.spark.sql.execution.ui // ATTENTION: spark package to access to the event.qe.executedPlan

import org.apache.spark.sql.execution._

object SparkInternal {
  def executedPlan(event: SparkListenerSQLExecutionEnd): SparkPlan = {
    event.qe.executedPlan
  }

  def queryExecution(event: SparkListenerSQLExecutionEnd): QueryExecution = {
    event.qe
  }

  /** Returns the full extended plan (logical + physical) when running live.
    * Returns None when replaying from an event log, where qe is not available.
    */
  def extendedDetails(event: SparkListenerSQLExecutionEnd): Option[String] =
    Option(event.qe).map(_.explainString(ExtendedMode))
}
