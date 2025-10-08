package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.SqlEntity
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{ExtendedMode, FormattedMode, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ui.SparkInternal

case class SqlReport(
  sqlId: Long,
  description: String,
  details: String,
  nodes: Seq[SqlNode]
) extends Report

object SqlReport extends Translator[SqlEntity, SqlReport] {

  /** Convert a [[Entity]] P into a collection of [[Report]] R
    *
    * @param c the configuration to perform the conversion
    * @param p the [[Entity]] to buildNodes
    * @return the collection of [[Report]] generated
    */
  override def fromEntityToReport(report: SqlEntity): SqlReport = {
    val details = describe(SparkInternal.queryExecution(report.end))
    SqlReport(
      sqlId = report.start.id,
      description = report.start.description,
      details = details,
      nodes = asNodes(report)
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
    (s._2.name.getOrElse(s._1), s._2.value.toString)

  private def asNodes(sqlEntity: SqlEntity): Seq[SqlNode] = {
    val sqlId = sqlEntity.start.id
    val plan = SparkInternal.executedPlan(sqlEntity.end)
    val nodes = buildNodes(sqlEntity.start.description, "0", sqlId, plan, "")
    nodes
  }
}