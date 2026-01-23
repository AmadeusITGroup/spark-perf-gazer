package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.SqlEvent
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo

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
    * @param metricsById the SQL metrics map keyed by accumulator id
    * @return the SqlReport generated
    */
  def apply(start: SqlEvent, metricsById: Map[Long, String]): SqlReport =
    SqlReport(
      sqlId = start.id,
      description = start.description,
      details = start.details,
      nodes = asNodes(start, metricsById)
    )

  private def buildNodes(
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
    val childNode = children.zipWithIndex.flatMap { case (pi, i) =>
      buildNodes(jobName, baseCoordinates + s".$i", sqlId, pi, planInfo.nodeName, metricsById)
    }
    Seq(currNode) ++ childNode
  }

  private def metricsForPlan(
    metrics: Seq[SQLMetricInfo],
    metricsById: Map[Long, String]
  ): Map[String, String] =
    metrics.map { metric =>
      val value = metricsById.getOrElse(metric.accumulatorId, "0")
      metric.name -> value
    }.toMap

  private def asNodes(start: SqlEvent, metricsById: Map[Long, String]): Seq[SqlNode] = {
    val sqlId = start.id
    buildNodes(start.description, "0", sqlId, start.planInfo, "", metricsById)
  }
}
