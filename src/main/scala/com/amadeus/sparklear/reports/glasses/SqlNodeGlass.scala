package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.{Report, SqlPlanNodeReport}

case class SqlNodeGlass(
  jobNameRegex: Option[String] = None,
  nodeNameRegex: Option[String] = None,
  parentNodeNameRegex: Option[String] = None,
  metricRegex: Option[String] = None,
  isLeaf: Option[Boolean] = None
) extends Glass {

  private def check(report: SqlPlanNodeReport): Boolean = {
    val j = jobNameRegex.map(r => report.jobName.matches(r)).getOrElse(true)
    val n = nodeNameRegex.map(r => report.nodeName.matches(r)).getOrElse(true)
    val p = parentNodeNameRegex.map(r => report.parentNodeName.matches(r)).getOrElse(true)
    val m = metricRegex.map(r => report.metrics.exists { case (n, _) => n.matches(r) }).getOrElse(true)
    val l = isLeaf.map(i => report.isLeaf == i).getOrElse(true)
    j && n && p && m && l
  }

  override def eligible(r: Report): Boolean = r match {
    case i: SqlPlanNodeReport => check(i)
    case _ => true
  }

}
