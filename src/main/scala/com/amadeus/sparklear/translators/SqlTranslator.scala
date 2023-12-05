package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.SqlPreReport
import com.amadeus.sparklear.reports.{Report, SqlNodeReport, StrReport}
import com.amadeus.sparklear.collects.SqlCollect
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait SqlTranslator[T <: Report] extends Translator[SqlPreReport, T]

case object SqlNodeTranslator extends SqlTranslator[SqlNodeReport] {

  private def resolveMetricInfo(m: SQLMetricInfo, ms: Map[Long, Long]): (String, String) = {
    val v = ms.get(m.accumulatorId)
    (m.name, v.getOrElse(-1).toString)
  }

  private def convert(
    jobName: String,
    baseCoord: String,
    sqlId: Long,
    plan: SparkPlanInfo,
    metrics: Map[Long, Long]
  ): Seq[SqlNodeReport] = {
    val currNode = SqlNodeReport(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = plan.nodeName,
      coordinates = baseCoord,
      metrics = plan.metrics.map(resolveMetricInfo(_, metrics))
    )
    val childNode = plan.children.zipWithIndex.flatMap { case (pi, i) =>
      convert(jobName, baseCoord + s".${i}", sqlId, pi, metrics)
    }
    Seq(currNode) ++ childNode
  }

  override def toAllReports(c: Config, preReport: SqlPreReport): Seq[SqlNodeReport] = {
    val metrics = preReport.metrics
    val sqlId = preReport.collect.id
    val plan = preReport.collect.plan
    val nodes = convert(preReport.collect.description, "0", sqlId, plan, metrics)
    nodes
  }
}

case object SqlPrettyTranslator extends SqlTranslator[StrReport] {
  override def toAllReports(c: Config, report: SqlPreReport): Seq[StrReport] =
    Seq(StrReport(SparkPlanInfoPrettifier.prettify(report.collect.plan, report.metrics)))
}
