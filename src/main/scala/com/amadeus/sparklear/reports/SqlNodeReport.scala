package com.amadeus.sparklear.reports

import com.amadeus.sparklear.reports.glasses.{Glass, SqlNodeGlass}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

case class SqlNodeReport(
  sqlId: Long,
  jobName: String,
  nodeName: String,
  coordinates: String,
  metrics: Seq[(String, String)]
) extends Report {
  override def asStringReport(): String = asJson(this)(DefaultFormats)

  private def check(g: SqlNodeGlass): Boolean = {
    val n = g.nodeNameRegex.map(r => nodeName.matches(r)).getOrElse(true)
    val m = g.metricRegex.map(r => metrics.exists { case (n, _) => n.matches(r) }).getOrElse(true)
    n && m
  }

  override def eligible(g: Glass): Boolean = g match {
    case i: SqlNodeGlass => check(i)
    case _ => true
  }
}
