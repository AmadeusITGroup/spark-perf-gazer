package com.amadeus.sparklear.reports

import com.amadeus.sparklear.reports.glasses.{Glass, SqlNodeGlass}

case class SqlNodeReport(
  sqlId: Long,
  name: String,
  level: Int,
  coord: String,
  metrics: Seq[(String, String)]
) extends Report {
  override def asStringReport(): String =
    s"SQL_ID=$sqlId NAME=${name} L=${level}, COORD=${coord} METRICS=${metrics.mkString(",")}"

  private def check(g: SqlNodeGlass): Boolean = {
    val n = g.nodeNameRegex.map(r => name.matches(r)).getOrElse(true)
    val m = g.metricRegex.map(r => metrics.exists{ case (n, _) => n.matches(r)}).getOrElse(true)
    n && m
  }

  override def eligible(g: Glass): Boolean = g match {
    case i: SqlNodeGlass => check(i)
    case _ => true
  }
}
