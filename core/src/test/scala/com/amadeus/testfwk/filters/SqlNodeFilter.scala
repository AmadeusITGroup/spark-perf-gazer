package com.amadeus.testfwk.filters

import com.amadeus.perfgazer.reports.{Report, SqlNode}

/**
  * Glass for SQL plan nodes
  *
  * @param jobNameRegex regex on the job name
  * @param nodeNameRegex regex on the node name (Join, Project, Scan, ...)
  * @param parentNodeNameRegex regex on the parent node name
  * @param metricRegex regex on the metric name (number of files read, ...)
  *                    a match in one metric is enough to keep the [[SqlNode]]
  * @param isLeaf expression to filter on leaf value
  */
case class SqlNodeFilter(
  jobNameRegex: Option[String] = None,
  nodeNameRegex: Option[String] = None,
  parentNodeNameRegex: Option[String] = None,
  metricRegex: Option[String] = None,
  isLeaf: Option[Boolean] = None
) extends Filter {

  def eligible(report: SqlNode): Boolean = {
    val j = jobNameRegex.map(r => report.jobName.matches(r)).getOrElse(true)
    val n = nodeNameRegex.map(r => report.nodeName.matches(r)).getOrElse(true)
    val p = parentNodeNameRegex.map(r => report.parentNodeName.matches(r)).getOrElse(true)
    val m = metricRegex.map(r => report.metrics.exists { case (n, _) => n.matches(r) }).getOrElse(true)
    val l = isLeaf.map(i => report.isLeaf == i).getOrElse(true)
    j && n && p && m && l
  }

}
