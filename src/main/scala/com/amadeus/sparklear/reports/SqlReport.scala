package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.SqlEntity
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ui.SparkInternal
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

case class SqlReport(
  details: String,
  nodes: Seq[SqlNode]
) extends Report {
  override def asJson: Json = toJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}

object SqlReport extends Translator[SqlEntity, SqlReport] {

  private def convert(
    jobName: String,
    baseCoord: String,
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
      nodeName = plan.nodeName,
      coordinates = baseCoord,
      metrics = metrics.map(metricToKv),
      isLeaf = children.isEmpty,
      parentNodeName = parentNodeName
    )
    val childNode = children.zipWithIndex.flatMap { case (pi, i) =>
      convert(jobName, baseCoord + s".${i}", sqlId, pi, plan.nodeName)
    }
    Seq(currNode) ++ childNode
  }

  /** Convert a [[Entity]] P into a collection of [[Report]] R
    *
    * @param c the configuration to perform the conversion
    * @param p the [[Entity]] to convert
    * @return the collection of [[Report]] generated
    */
  override def fromEntityToReport(report: SqlEntity): SqlReport =
    SqlReport(
      details = prettify(SparkInternal.executedPlan(report.end)),
      nodes = asNodes(report)
    )

  private def prettify(planInfo: SparkPlan, indent: String = ""): String = {
    val builder = new StringBuilder()
    builder.append(s"${indent}Operator ${planInfo.nodeName}\n")
    //if (planInfo.metadata.nonEmpty) {
    //  builder.append(s"${indent}- Metadata: ${planInfo.metadata}\n")
    //}
    if (planInfo.metrics.nonEmpty) {
      builder.append(s"${indent}- Metrics: ${planInfo.metrics.map(metricToKv).mkString(",")}\n")
    }
    // Recursively prettify children
    planInfo.children.foreach { childInfo =>
      builder.append(prettify(childInfo, s"$indent      "))
    }
    builder.toString()
  }

  private def metricToKv(s: (String, SQLMetric)): (String, String) =
    (s._2.name.getOrElse(s._1), s._2.value.toString)

  private def asNodes(sqlEntity: SqlEntity): Seq[SqlNode] = {
    val sqlId = sqlEntity.start.id
    val plan = SparkInternal.executedPlan(sqlEntity.end)
    val nodes = convert(sqlEntity.start.description, "0", sqlId, plan, "")
    nodes
  }

}
