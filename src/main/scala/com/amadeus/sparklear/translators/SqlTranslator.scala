package com.amadeus.sparklear.translators

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.prereports.SqlPreReport
import com.amadeus.sparklear.reports.{Report, StrReport, SqlNodeReport}
import com.amadeus.sparklear.collects.SqlCollect
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait SqlTranslator[T <: Report] extends Translator[SqlPreReport, T]

case object SqlJsonFlat extends SqlTranslator[SqlNodeReport] {
  private def convert(baseCoord: String, level: Int, sqlId: Long, p: SparkPlanInfo, m: Map[Long, Long]): Seq[SqlNodeReport] = {
    val currNode = SqlNodeReport(
      sqlId = sqlId,
      name = p.nodeName,
      level = level,
      coord = baseCoord,
      metrics = p.metrics.map(convert(_, m))
    )
    val childNode = p.children.zipWithIndex.flatMap{case (pi, i) => convert(baseCoord + s".${i}" ,level + 1, sqlId, pi, m)}
    Seq(currNode) ++ childNode
  }

  private def convert(m: SQLMetricInfo, ms: Map[Long, Long]): (String, String) = {
    val v = ms.get(m.accumulatorId)
    (m.name, v.getOrElse(-1).toString)
  }

  override def toReport(c: Config, r: SqlPreReport): Seq[SqlNodeReport] = {
    val metrics = r.m
    val sqlId = r.w.id
    val plan = r.w.p
    val nodes = convert("0", 1, sqlId, plan, metrics)
    nodes.filter(n => c.glasses.forall(g => n.eligible(g)))
  }
}

case object SqlJson extends SqlTranslator[StrReport] {

  private def convert(p: SparkPlanInfo, m: Map[Long, Long]): SparkPlanInfo = new SparkPlanInfo(
    nodeName = p.nodeName,
    simpleString = p.simpleString,
    children = p.children.map(convert(_, m)),
    metadata = p.metadata,
    metrics = p.metrics.map(convert(_, m))
  )
  private def convert(m: SQLMetricInfo, ms: Map[Long, Long]): SQLMetricInfo = {
    val v = ms.get(m.accumulatorId)
    new SQLMetricInfo(
      name = m.name,
      accumulatorId = v.getOrElse(-1),
      metricType = v.map(_ => "OK").getOrElse("KO")
    )
  }

  override def toReport(c: Config, r: SqlPreReport): Seq[StrReport] = {
    val m = r.m
    val p = r.w
    val newP = SqlCollect(
      id = p.id,
      p = convert(p.p, m)
    )
    Seq(StrReport(asJson(newP)(DefaultFormats)))
  }
}

case object SqlPretty extends SqlTranslator[StrReport] {
  override def toReport(c: Config, r: SqlPreReport): Seq[StrReport] =
    Seq(StrReport(SparkPlanInfoPrettifier.prettify(r.w.p, r.m)))
}

case object SqlSingleLine extends SqlTranslator[StrReport] {
  override def toReport(c: Config, r: SqlPreReport): Seq[StrReport] =
    Seq(StrReport(SparkPlanInfoPrettifier.prettifySingleLine(r.w.id, r.w.p)))
}
