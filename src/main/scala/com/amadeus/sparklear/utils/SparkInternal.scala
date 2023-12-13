package org.apache.spark.sql.execution.ui // ATTENTION: spark package to access to the event.qe.executedPlan

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExecutedCommandExec
import org.apache.spark.sql.execution.datasources.v2.OverwriteByExpressionExec
import org.apache.spark.sql.execution.metric.SQLMetric
import org.slf4j.{Logger, LoggerFactory}

object SparkInternal {

  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val Empty = Map.empty[Long, Long]

  def executedPlanMetrics(event: SparkListenerSQLExecutionEnd): Map[Long, Long] = {
    planToMetrics(event.qe.executedPlan)
  }

  private def planToMetrics(p: SparkPlan): Map[Long, Long] = {
    val node = p match {
      case i: FileSourceScanExec =>
        toMetric(i.metrics) ++
          toMetric(i.driverMetrics) // not sure why i.driverMetrics not included in i.metrics
      case i: ProjectExec => toMetric(i.metrics) // always empty?
      case i: ExecutedCommandExec => toMetric(i.metrics) // always empty?
      case i: SerializeFromObjectExec => toMetric(i.metrics) // always empty?
      case i: MapElementsExec => toMetric(i.metrics) // always empty?
      case i: InputAdapter => toMetric(i.metrics) // always empty?
      case i: DeserializeToObjectExec => toMetric(i.metrics) // always empty?
      case i: MapPartitionsExec => toMetric(i.metrics) // always empty?
      case i: OverwriteByExpressionExec => toMetric(i.metrics) // always empty?
      case i if i.metrics.nonEmpty => toMetric(i.metrics)
      case i => {
        logger.trace(s"${this.getClass.getName}.planToMetrics(${i.getClass.getName}): no metrics reported")
        Empty
      }
    }
    node ++ p.children.map(planToMetrics).reduceOption(_ ++ _).getOrElse(Empty)
  }

  private def toMetric(m: Map[String, SQLMetric]): Map[Long, Long] = {
    m.values.map(i => (i.id, i.value)).toMap
  }
}
