package org.apache.spark.sql.execution.ui

import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.v2.V2ExistingTableWriteExec
import org.apache.spark.sql.execution.metric.SQLMetric

object SparkInternal {
  def executedPlanMetrics(event: SparkListenerSQLExecutionEnd): Map[Long, Long] = {
    val ep = event.qe.executedPlan
    val metrics = ep match {
      case i: V2ExistingTableWriteExec => resolvePlan(i.query)
      case _ =>
        throw new IllegalStateException(s"Not supported executedPlanMetrics(${ep.getClass.getName})")
    }
    metrics
  }

  private def resolvePlan(p: SparkPlan): Map[Long, Long] = {
    val node = p match {
      case i: FileSourceScanExec => toMetric(i.driverMetrics) ++ toMetric(i.metrics)
      case _ => throw new IllegalStateException(s"Not supported resolvePlan(${p.getClass.getName})")
    }
    node ++ p.children.map(resolvePlan).reduceOption(_ ++ _).getOrElse(Map.empty[Long, Long])
  }

  private def toMetric(m: Map[String, SQLMetric]): Map[Long, Long] = {
    m.values.map(i => (i.id, i.value)).toMap
  }
}
