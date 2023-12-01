package com.amadeus.sparklear.input.converters

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.input.{Output, SqlInput}
import com.amadeus.sparklear.input.converters.Serializer.{OutputString, StringReport}
import com.amadeus.sparklear.wrappers.SqlWrapper
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait SqlSerializer[T <: Output] extends Serializer[SqlInput, T]

case class SqlJsonFlatNode(sqlId: Long, name: String, level: Int, coord: String, metrics: Seq[(String, String)]) extends Output {
  override def asString(): String = s"SQL_ID=$sqlId NAME=${name} L=${level}, COORD=${coord} METRICS=${metrics.mkString(",")}"
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
    val childNode = p.children.zipWithIndex.flatMap{case (pi, i) => convert(baseCoord + s".${i}" ,level + 1, sqlId, pi, m)}
    Seq(currNode) ++ childNode
  }

  private def convert(m: SQLMetricInfo, ms: Map[Long, Long]): (String, String) = {
    val v = ms.get(m.accumulatorId)
    (m.name, v.getOrElse(-1).toString)
  }

  override def toOutput(c: Config, r: SqlInput): Seq[SqlJsonFlatNode] = {
    val m = r.m
    val id = r.w.id
    val p = r.w.p
    convert("0", 1, id, p, m)
  }
}

case object SqlJson extends SqlSerializer[OutputString] {

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

  override def toOutput(c: Config, r: SqlInput): Seq[OutputString] = {
    val m = r.m
    val p = r.w
    val newP = SqlWrapper(
      id = p.id,
      p = convert(p.p, m)
    )
    Seq(OutputString(asJson(newP)(DefaultFormats)))
  }
}

case object SqlPretty extends SqlSerializer[OutputString] {
  override def toOutput(c: Config, r: SqlInput): Seq[OutputString] =
    Seq(OutputString(SparkPlanInfoPrettifier.prettify(r.w.p, r.m)))
}

case object SqlSingleLine extends SqlSerializer[OutputString] {
  override def toOutput(c: Config, r: SqlInput): Seq[OutputString] =
    Seq(OutputString(SparkPlanInfoPrettifier.prettifySingleLine(r.w.id, r.w.p)))
}
