package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.translators.{JobTranslator, SqlTranslator, StageTranslator}
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.reports.glasses.{Glass, SqlNodeGlass}

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(stringReportPrefix = p)
    def withSqlSerializer(s: SqlTranslator[_ <: Report]): Config = c.copy(sqlSerializer = s)
    def withJobSerializer(s: JobTranslator[_ <: Report]): Config = c.copy(jobSerializer = s)
    def withStageSerializer(s: StageTranslator): Config = c.copy(stageSerializer = s)
    def withAllEnabled: Config = c.copy(showSqls = true, showJobs = true, showStages = true)
    def withOnlySqlEnabled: Config = c.copy(showSqls = true, showJobs = false, showStages = false)
    def withGlasses(g: Seq[Glass]): Config = c.copy(glasses = g)
    def withStringSink(ss: String => Unit): Config = c.copy(stringReportSink = Some(ss))
    def withReportSink(ss: Report => Unit): Config = c.copy(reportSink = Some(ss))
    def withPreReportSink(ss: PreReport => Unit): Config = c.copy(preReportSink = Some(ss))
  }

  def defaultTestConfig: Config = {
    Config().withPrefix("")
  }


}
