package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.wrappers.SqlWrapper
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

sealed trait SqlSerializer {
  def output(p: SqlWrapper, m: Map[Long, Long]): String
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

  override def output(p: SqlWrapper, m: Map[Long, Long]): String = {
    val newP = SqlWrapper(
      id = p.id,
      p = convert(p.p, m)
    )
    asJson(newP)(DefaultFormats)
  }
}

case object SqlPretty extends SqlSerializer {
  override def output(p: SqlWrapper, m: Map[Long, Long]): String =
    SparkPlanInfoPrettifier.prettify(p.p, m)
}
