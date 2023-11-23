package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.reports.Report.StringReport
import com.amadeus.sparklear.reports.SqlReport
import com.amadeus.sparklear.wrappers.SqlWrapper
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait SqlSerializer {
  def output(p: SqlReport): String
}

case object SqlJson extends SqlSerializer {

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

  override def output(r: SqlReport): StringReport = {
    val m = r.m
    val p = r.w
    val newP = SqlWrapper(
      id = p.id,
      p = convert(p.p, m)
    )
    asJson(newP)(DefaultFormats)
  }
}

case object SqlPretty extends SqlSerializer {
  override def output(r: SqlReport): StringReport =
    SparkPlanInfoPrettifier.prettify(r.w.p, r.m)
}

case object SqlSingleLine extends SqlSerializer {
  override def output(r: SqlReport): StringReport =
    SparkPlanInfoPrettifier.prettifySingleLine(r.w.id, r.w.p)
}
