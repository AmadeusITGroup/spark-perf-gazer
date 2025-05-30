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
}

case class StrJobReport(s: String) extends StrReport {
  override def entity: EntityName = EntityNameJob
}

case class StrStageReport(s: String) extends StrReport {
  override def entity: EntityName = EntityNameStage
}
