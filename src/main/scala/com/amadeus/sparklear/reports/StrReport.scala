package com.amadeus.sparklear.reports
import com.amadeus.sparklear.translators.JobTranslator.EntityNameJob
import com.amadeus.sparklear.translators.SqlTranslator.EntityNameSql
import com.amadeus.sparklear.translators.StageTranslator.EntityNameStage
import com.amadeus.sparklear.translators.Translator.EntityName

trait StrReport extends Report {
  def s: String
}

case class StrSqlReport(s: String) extends StrReport {
  override def entity: EntityName = EntityNameSql
  override def asStringReport(): String = s
}

case class StrJobReport(s: String) extends StrReport {
  override def entity: EntityName = EntityNameJob
  override def asStringReport(): String = s
}

case class StrStageReport(s: String) extends StrReport {
  override def entity: EntityName = EntityNameStage
  override def asStringReport(): String = s
}
