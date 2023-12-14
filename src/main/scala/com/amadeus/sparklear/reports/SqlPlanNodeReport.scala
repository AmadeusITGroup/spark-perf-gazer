package com.amadeus.sparklear.reports

import com.amadeus.sparklear.translators.SqlTranslator.EntityNameSql
import com.amadeus.sparklear.translators.Translator.{EntityName, StringReport}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => asJson}

case class SqlPlanNodeReport(
  sqlId: Long,
  jobName: String,
  nodeName: String,
  coordinates: String,
  metrics: Map[String, String],
  isLeaf: Boolean,
  parentNodeName: String,
) extends Report {
  override def entity: EntityName = EntityNameSql
  override def asStringReport(): StringReport = asJson(this)(DefaultFormats)
}
