package com.amadeus.testfwk

import com.amadeus.sparklear.{SparklearConfig, SparklearConfigParams}

trait ConfigSupport {
  val defaultMaxCacheSize: Int = 200

  implicit class ConfigHelper(c: SparklearConfig) {
    def withAllEnabled: SparklearConfig = c.copy(sqlEnabled = true, jobsEnabled = true, stagesEnabled = true, tasksEnabled = true)

    def withAllDisabled: SparklearConfig = c.copy(sqlEnabled = false, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
    def withOnlySqlEnabled: SparklearConfig = c.copy(sqlEnabled = true, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
  }

  def defaultTestConfig: SparklearConfig = SparklearConfig.build(SparklearConfigParams(defaultMaxCacheSize = defaultMaxCacheSize))
}
