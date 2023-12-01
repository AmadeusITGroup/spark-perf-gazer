package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.reports.SqlReport
import com.amadeus.sparklear.wrappers.SqlWrapper
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait SqlSerializer[T] {
  def prepare(c: Config, p: SqlReport): Seq[T] = Seq.empty[T]
  def output(c: Config, p: SqlReport): String
}

case class SqlJsonFlatNode(sqlId: Long, name: String, level: Int, coord: String, metrics: Seq[String]) /*extends Report*/ {
  override def toString(): String = s"SQL_ID=$sqlId NAME=${name} L=${level}, COORD=${coord} METRICS=${metrics.mkString(",")}"
}

case object SqlJsonFlat extends SqlSerializer[SqlJsonFlatNode] {
  private def convert(baseCoord: String, level: Int, sqlId: Long, p: SparkPlanInfo, m: Map[Long, Long]): Seq[SqlJsonFlatNode] = {
    val currNode = SqlJsonFlatNode(
      sqlId = sqlId,
      name = p.nodeName,
      level = level,
      coord = baseCoord,
      metrics = p.metrics.map(convert(_, m))
    )
    val childNode: Seq[SqlJsonFlatNode] = p.children.zipWithIndex.flatMap{case (pi, i) => convert(baseCoord + s".${i}" ,level + 1, sqlId, pi, m)}
    val k: Seq[SqlJsonFlatNode] = Seq(currNode) ++ childNode
    k
  }

  private def convert(m: SQLMetricInfo, ms: Map[Long, Long]): String = {
    val name = m.name.replace(' ', '_')
    val v = ms.get(m.accumulatorId)
    s"${name}=${v.getOrElse(-1)}"
  }

  override def prepare(c: Config, r: SqlReport): Seq[SqlJsonFlatNode] = {
    val m = r.m
    val id = r.w.id
    val p = r.w.p
    convert("0", 1, id, p, m)
  }
  override def output(c: Config, r: SqlReport): StringReport = {
    prepare(c, r).map(_.toString()).mkString("\n")
  }
}

case object SqlJson extends SqlSerializer[Nothing] {

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

  override def output(c: Config, r: SqlReport): StringReport = {
    val m = r.m
    val p = r.w
    val newP = SqlWrapper(
      id = p.id,
      p = convert(p.p, m)
    )
    asJson(newP)(DefaultFormats)
  }
}

case object SqlPretty extends SqlSerializer[Nothing] {
  override def output(c: Config, r: SqlReport): StringReport =
    SparkPlanInfoPrettifier.prettify(r.w.p, r.m)
}

case object SqlSingleLine extends SqlSerializer[Nothing] {
  override def output(c: Config, r: SqlReport): StringReport =
    SparkPlanInfoPrettifier.prettifySingleLine(r.w.id, r.w.p)
}
