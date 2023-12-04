package com.amadeus.sparklear.report.glasses

case class SqlNodeGlass(
  nodeNameRegex: Option[String] = None,
  metricRegex: Option[String] = None
) extends Glass
