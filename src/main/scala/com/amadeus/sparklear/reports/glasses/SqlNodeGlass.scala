package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{Report, SqlNodeReport}

case class SqlNodeGlass(
  nodeNameRegex: Option[String] = None,
  metricRegex: Option[String] = None
) extends Glass {


  private def check(report: SqlNodeReport): Boolean = {
    val n = nodeNameRegex.map(r => report.nodeName.matches(r)).getOrElse(true)
    val m = metricRegex.map(r => report.metrics.exists { case (n, _) => n.matches(r) }).getOrElse(true)
    n && m
  }

  override def eligible(r: Report): Boolean = r match {
    case i: SqlNodeReport => check(i)
    case _ => true
  }

}
