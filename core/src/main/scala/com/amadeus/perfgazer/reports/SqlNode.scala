package com.amadeus.perfgazer.reports

case class SqlNode(
  sqlId: Long,
  jobName: String,
  nodeName: String,
  coordinates: String,
  metrics: Map[String, String],
  isLeaf: Boolean,
  parentNodeName: String
)
