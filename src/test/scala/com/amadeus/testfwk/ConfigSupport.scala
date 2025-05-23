package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.translators.{JobTranslator, SqlTranslator, StageTranslator}
import com.amadeus.sparklear.entities.Entity
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.reports.filters.{Filter, SqlNodeFilter}
import com.amadeus.sparklear.translators.Translator.TranslatorName

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(stringReportPrefix = p)
    def withSqlTranslator(s: TranslatorName): Config = c.copy(sqlTranslatorName = s)
    def withJobTranslator(s: TranslatorName): Config = c.copy(jobTranslatorName = s)
    def withStageTranslator(s: TranslatorName): Config = c.copy(stageTranslatorName = s)
    def withAllEnabled: Config = c.copy(sqlEnabled = true, jobsEnabled = true, stagesEnabled = true)

    def withAllDisabled: Config = c.copy(sqlEnabled = false, jobsEnabled = false, stagesEnabled = false)
    def withOnlySqlEnabled: Config = c.copy(sqlEnabled = true, jobsEnabled = false, stagesEnabled = false)
    def withFilters(g: Seq[Filter]): Config = c.copy(filters = g)
    def withStringSink(ss: String => Unit): Config = c.copy(stringReportSink = Some(ss))
    def withReportSink(ss: Report => Unit): Config = c.copy(reportSink = Some(ss))
    def withPreReportSink(ss: Entity => Unit): Config = c.copy(preReportSink = Some(ss))
  }

  def defaultTestConfig: Config = {
    Config().withPrefix("")
  }


}
