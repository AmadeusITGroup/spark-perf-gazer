package com.amadeus.sparklear.reports.glasses

case class SqlNodeGlass(
  nodeNameRegex: Option[String] = None,
  metricRegex: Option[String] = None
) extends Glass
