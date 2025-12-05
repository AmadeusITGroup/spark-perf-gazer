package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.SqlEvent
import org.apache.spark.sql.execution.{ExtendedMode, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ui.{SparkInternal, SparkListenerSQLExecutionEnd}

case class SqlReport(
  sqlId: Long,
  description: String,
  details: String,
  nodes: Seq[SqlNode]
) extends Report {
  override def reportType: ReportType = SqlReportType
}

object SqlReport {

  /** Create a SqlReport
    *
    * @param start the SqlEvent for SQL execution start
    * @param end the SparkListenerSQLExecutionEnd for SQL execution end
    * @return the SqlReport generated
    */
  def apply(start: SqlEvent, end: SparkListenerSQLExecutionEnd): SqlReport = {
    val details = describe(SparkInternal.queryExecution(end))
    SqlReport(
      sqlId = start.id,
      description = start.description,
      details = details,
      nodes = asNodes(start, end)
    )
  }

  private def buildNodes(
    jobName: String,
    baseCoordinates: String,
    sqlId: Long,
    plan: SparkPlan,
    parentNodeName: String
  ): Seq[SqlNode] = {

    val (children, metrics) = plan match {
      case a: AdaptiveSparkPlanExec => (a.finalPhysicalPlan.children, a.finalPhysicalPlan.metrics)
      case a: ShuffleQueryStageExec => (a.shuffle.children, a.shuffle.metrics)
      case x => (x.children, x.metrics)
    }

    val currNode = SqlNode(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = s"() ${plan.nodeName}",
      coordinates = baseCoordinates,
      metrics = metrics.map(metricToKv),
      isLeaf = children.isEmpty,
      parentNodeName = parentNodeName
    )
    val childNode = children.zipWithIndex.flatMap { case (pi, i) =>
      buildNodes(jobName, baseCoordinates + s".$i", sqlId, pi, plan.nodeName)
    }
    Seq(currNode) ++ childNode
  }

  private def describe(qe: QueryExecution): String = {
    val s = qe.explainString(ExtendedMode) // TODO check formatted as well
    s
  }

  private def metricToKv(s: (String, SQLMetric)): (String, String) =
    ( s._2.name.getOrElse(
        // $COVERAGE-OFF$ A SQLMetric should always have a name as long as it has been registered
        s._1
        // $COVERAGE-ON$
      ), s._2.value.toString
    )

  private def asNodes(start: SqlEvent, end: SparkListenerSQLExecutionEnd): Seq[SqlNode] = {
    val sqlId = start.id
    val plan = SparkInternal.executedPlan(end)
    val nodes = buildNodes(start.description, "0", sqlId, plan, "")
    nodes
  }
}
