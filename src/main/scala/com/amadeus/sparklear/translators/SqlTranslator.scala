package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.SqlPreReport
import com.amadeus.sparklear.reports.{Report, SqlPlanNodeReport, StrReport, StrSqlReport}
import com.amadeus.sparklear.collects.SqlCollect
import com.amadeus.sparklear.translators.Translator.{EntitySql, TranslatorName}
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

object SqlTranslator {
  val Translators: Seq[SqlTranslator[_ <: Report]] = Seq(SqlPlanNodeTranslator, SqlPrettyTranslator)
  def forName(s: TranslatorName): SqlTranslator[_ <: Report] = Translator.forName(Translators)(EntitySql, s)
}
sealed trait SqlTranslator[T <: Report] extends Translator[SqlPreReport, T]

case object SqlPlanNodeTranslator extends SqlTranslator[SqlPlanNodeReport] {
  final val ValueNotFoundForMetricKeyAcumValue = -1

  def name: TranslatorName = "sqlplannode"

  private def resolveMetricInfo(m: SQLMetricInfo, ms: Map[Long, Long]): (String, String) = {
    val v = ms.get(m.accumulatorId)
    (m.name, v.getOrElse(ValueNotFoundForMetricKeyAcumValue).toString)
  }

  private def convert(
    jobName: String,
    baseCoord: String,
    sqlId: Long,
    plan: SparkPlanInfo,
    metrics: Map[Long, Long],
    parentNodeName: String
  ): Seq[SqlPlanNodeReport] = {
    val currNode = SqlPlanNodeReport(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = plan.nodeName,
      coordinates = baseCoord,
      metrics = plan.metrics.map(resolveMetricInfo(_, metrics)),
      isLeaf = plan.children.isEmpty,
      parentNodeName = parentNodeName
    )
    val childNode = plan.children.zipWithIndex.flatMap { case (pi, i) =>
      convert(jobName, baseCoord + s".${i}", sqlId, pi, metrics, plan.nodeName)
    }
    Seq(currNode) ++ childNode
  }

  override def toAllReports(c: Config, preReport: SqlPreReport): Seq[SqlPlanNodeReport] = {
    val metrics = preReport.metrics
    val sqlId = preReport.collect.id
    val plan = preReport.collect.plan
    val nodes = convert(preReport.collect.description, "0", sqlId, plan, metrics, "")
    nodes
  }
}

case object SqlPrettyTranslator extends SqlTranslator[StrReport] {

  def name: TranslatorName = "sqlpretty"
  override def toAllReports(c: Config, report: SqlPreReport): Seq[StrReport] =
    Seq(StrSqlReport(SparkPlanInfoPrettifier.prettify(report.collect.plan, report.metrics)))
}
