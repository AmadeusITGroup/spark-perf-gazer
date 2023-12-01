package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.reports.converters.{JobSerializer, SqlSerializer, StageSerializer}

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(prefix = p)
    def withSqlSerializer(s: SqlSerializer[_]): Config = c.copy(sqlSerializer = s)
    def withJobSerializer(s: JobSerializer): Config = c.copy(jobSerializer = s)
    def withStageSerializer(s: StageSerializer): Config = c.copy(stageSerializer = s)
    def withAllEnabled: Config = c.copy(showSqls = true, showJobs = true, showStages = true)
  }

  def defaultTestConfig: Config = {
    Config.DefaultConfig.withPrefix("")
  }


}
