package com.amadeus.sparklear.reports
import com.amadeus.sparklear.translators.Translator.{EntityJob, EntityName, EntitySql, EntityStage}

trait StrReport extends Report {
  def s: String
}

case class StrSqlReport(s: String) extends StrReport {
  override def entity: EntityName = EntitySql
  override def asStringReport(): String = s
}

case class StrJobReport(s: String) extends StrReport {
  override def entity: EntityName = EntityJob
  override def asStringReport(): String = s
}

case class StrStageReport(s: String) extends StrReport {
  override def entity: EntityName = EntityStage
  override def asStringReport(): String = s
}
