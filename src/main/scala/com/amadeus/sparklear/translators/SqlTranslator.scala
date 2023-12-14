package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.SqlPreReport
import com.amadeus.sparklear.reports.{Report, SqlPlanNodeReport, StrReport, StrSqlReport}
import com.amadeus.sparklear.collects.SqlCollect
import com.amadeus.sparklear.translators.SqlTranslator.metricToKv
import com.amadeus.sparklear.translators.Translator.{EntityName, TranslatorName}
import org.apache.spark.sql.execution.{SparkPlan, SparkPlanInfo}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetricInfo}
import org.apache.spark.sql.execution.ui.SparkInternal
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

object SqlTranslator {
  val EntityNameSql: EntityName = "SQL"
  val Translators: Seq[SqlTranslator[_ <: Report]] = Seq(SqlPlanNodeTranslator, SqlPrettyTranslator)
  def forName(s: TranslatorName): SqlTranslator[_ <: Report] = Translator.forName(Translators)(EntityNameSql, s)

  def metricToKv(s: (String, SQLMetric)): (String, String) = (s._2.name.getOrElse(s._1), s._2.value.toString)
}
sealed trait SqlTranslator[T <: Report] extends Translator[SqlPreReport, T]

case object SqlPlanNodeTranslator extends SqlTranslator[SqlPlanNodeReport] {

  def name: TranslatorName = "sqlplannode"

  private def convert(
    jobName: String,
    baseCoord: String,
    sqlId: Long,
    plan: SparkPlan,
    parentNodeName: String
  ): Seq[SqlPlanNodeReport] = {
    val currNode = SqlPlanNodeReport(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = plan.nodeName,
      coordinates = baseCoord,
      metrics = plan.metrics.map(metricToKv),
      isLeaf = plan.children.isEmpty,
      parentNodeName = parentNodeName
    )
    val childNode = plan.children.zipWithIndex.flatMap { case (pi, i) =>
      convert(jobName, baseCoord + s".${i}", sqlId, pi, plan.nodeName)
    }
    Seq(currNode) ++ childNode
  }

  override def toAllReports(c: Config, preReport: SqlPreReport): Seq[SqlPlanNodeReport] = {
    val sqlId = preReport.collect.id
    val plan = SparkInternal.executedPlan(preReport.end)
    val nodes = convert(preReport.collect.description, "0", sqlId, plan, "")
    nodes
  }
}

case object SqlPrettyTranslator extends SqlTranslator[StrReport] {

  def name: TranslatorName = "sqlpretty"
  override def toAllReports(c: Config, report: SqlPreReport): Seq[StrReport] =
    Seq(StrSqlReport(prettify(SparkInternal.executedPlan(report.end))))

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
}
