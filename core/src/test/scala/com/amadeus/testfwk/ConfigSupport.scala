package com.amadeus.testfwk

import com.amadeus.sparklear.SparklearConfig

trait ConfigSupport {

  val DefaultMaxCacheSize: Int = 200

  implicit class ConfigHelper(c: SparklearConfig) {
    def withAllEnabled: SparklearConfig = c.copy(sqlEnabled = true, jobsEnabled = true, stagesEnabled = true, tasksEnabled = true)

    def withAllDisabled: SparklearConfig = c.copy(sqlEnabled = false, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
    def withOnlySqlEnabled: SparklearConfig = c.copy(sqlEnabled = true, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
  }

  def defaultTestConfig: SparklearConfig = SparklearConfig(maxCacheSize = DefaultMaxCacheSize)

}
