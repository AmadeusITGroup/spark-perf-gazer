package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.reports.converters.{JobSerializer, SqlSerializer, StageSerializer}

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(prefix = p)
    def withSqlSerializer(s: SqlSerializer): Config = c.copy(sqlSerializer = s)
    def withJobSerializer(s: JobSerializer): Config = c.copy(jobSerializer = s)
    def withStageSerializer(s: StageSerializer): Config = c.copy(stageSerializer = s)
  }

  def defaultTestConfig: Config = {
    Config.DefaultConfig.withPrefix("")
  }


}
