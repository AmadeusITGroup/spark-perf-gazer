package com.amadeus.sparklear.reports

import com.amadeus.sparklear.translators.Translator.{EntityName, EntitySql, StringReport}
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
  override def entity: EntityName = EntitySql
  override def asStringReport(): StringReport = asJson(this)(DefaultFormats)
}
