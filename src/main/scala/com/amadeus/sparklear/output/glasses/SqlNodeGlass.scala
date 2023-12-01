package com.amadeus.sparklear.output.glasses

case class SqlNodeGlass(
  nodeNameRegex: Option[String] = None,
  metricRegex: Option[String] = None
) extends Glass
