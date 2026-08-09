package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.SqlEvent
import com.amadeus.perfgazer.schema.{ColumnDoc, TableDoc}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanInfo}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetricInfo}
import org.apache.spark.sql.execution.ui.{SparkInternal, SparkListenerSQLExecutionEnd}

@TableDoc(name = "sql", description = "SQL query execution report with SQL plans (logical, physical, ...) and their node metrics. One row per completed SQL execution.")
case class SqlReport(
  @ColumnDoc(description = "Unique SQL execution identifier")
  sqlId: Long,
  @ColumnDoc(description = "SQL query description")
  description: String,
  @ColumnDoc(description = "Extended query execution plan")
  details: String,
  @ColumnDoc(description = "Physical plan nodes with execution metrics")
  nodes: Seq[SqlNode]
) extends Report {
  override def reportType: ReportType = SqlReportType
}

object SqlReport {

  /** Create a SqlReport in live mode from the SQL execution end event.
    * Uses the live SparkPlan and its SQLMetric values directly.
    */
  def apply(start: SqlEvent, end: SparkListenerSQLExecutionEnd): SqlReport = {
    val qe = SparkInternal.queryExecution(end)
    val details = qe.explainString(org.apache.spark.sql.execution.ExtendedMode)
    SqlReport(
      sqlId = start.id,
      description = start.description,
      details = details,
      nodes = nodesFromPlan(start, end)
    )
  }

  /** Create a SqlReport in replay mode from serialized plan info and accumulated metrics.
    */
  def apply(start: SqlEvent, metricsById: Map[Long, String]): SqlReport =
    SqlReport(
      sqlId = start.id,
      description = start.description,
      details = start.details,
      nodes = nodesFromPlanInfo(start, metricsById)
    )

  // --- Live mode: walk the SparkPlan tree directly ---

  private def nodesFromPlan(start: SqlEvent, end: SparkListenerSQLExecutionEnd): Seq[SqlNode] = {
    val plan = SparkInternal.executedPlan(end)
    buildNodesFromPlan(start.description, "0", start.id, plan, "")
  }

  private def buildNodesFromPlan(
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
    val childNodes = children.zipWithIndex.flatMap { case (child, i) =>
      buildNodesFromPlan(jobName, baseCoordinates + s".$i", sqlId, child, plan.nodeName)
    }
    Seq(currNode) ++ childNodes
  }

  private def metricToKv(s: (String, SQLMetric)): (String, String) =
    (
      s._2.name.getOrElse(
        // $COVERAGE-OFF$ A SQLMetric should always have a name as long as it has been registered
        s._1
        // $COVERAGE-ON$
      ),
      s._2.value.toString
    )

  // --- Replay mode: walk SparkPlanInfo with metrics looked up by accumulator ID ---

  private def nodesFromPlanInfo(start: SqlEvent, metricsById: Map[Long, String]): Seq[SqlNode] = {
    buildNodesFromPlanInfo(start.description, "0", start.id, start.planInfo, "", metricsById)
  }

  private def buildNodesFromPlanInfo(
    jobName: String,
    baseCoordinates: String,
    sqlId: Long,
    planInfo: SparkPlanInfo,
    parentNodeName: String,
    metricsById: Map[Long, String]
  ): Seq[SqlNode] = {
    val children = planInfo.children

    val currNode = SqlNode(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = s"() ${planInfo.nodeName}",
      coordinates = baseCoordinates,
      metrics = metricsForPlan(planInfo.metrics, metricsById),
      isLeaf = children.isEmpty,
      parentNodeName = parentNodeName
    )
    val childNodes = children.zipWithIndex.flatMap { case (pi, i) =>
      buildNodesFromPlanInfo(jobName, baseCoordinates + s".$i", sqlId, pi, planInfo.nodeName, metricsById)
    }
    Seq(currNode) ++ childNodes
  }

  private def metricsForPlan(
    metrics: Seq[SQLMetricInfo],
    metricsById: Map[Long, String]
  ): Map[String, String] =
    metrics.map { metric =>
      val value = metricsById.getOrElse(metric.accumulatorId, "0")
      metric.name -> value
    }.toMap
}
