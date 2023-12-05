package com.amadeus.sparklear.reports

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

case class SqlPlanNodeReport(
  sqlId: Long,
  jobName: String,
  nodeName: String,
  coordinates: String,
  metrics: Seq[(String, String)],
  isLeaf: Boolean,
  parentNodeName: String,
) extends Report {
  override def asStringReport(): String = asJson(this)(DefaultFormats)
}
