package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.converters.{JobReporter, SqlReporter, StageReporter}
import com.amadeus.sparklear.input.Input
import com.amadeus.sparklear.report.Report
import com.amadeus.sparklear.report.glasses.{Glass, SqlNodeGlass}

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(prefix = p)
    def withSqlSerializer(s: SqlReporter[_ <: Report]): Config = c.copy(sqlSerializer = s)
    def withJobSerializer(s: JobReporter): Config = c.copy(jobSerializer = s)
    def withStageSerializer(s: StageReporter): Config = c.copy(stageSerializer = s)
    def withAllEnabled: Config = c.copy(showSqls = true, showJobs = true, showStages = true)
    def withGlasses(g: Seq[Glass]): Config = c.copy(glasses = g)
    def withStringSink(ss: String => Unit): Config = c.copy(stringSink = Some(ss))
    def withOutputSink(ss: Report => Unit): Config = c.copy(outputSink = Some(ss))
    def withInputSink(ss: Input => Unit): Config = c.copy(inputSink = Some(ss))
  }

  def defaultTestConfig: Config = {
    Config().withPrefix("")
  }


}
