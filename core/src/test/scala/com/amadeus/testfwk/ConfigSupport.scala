package com.amadeus.testfwk

import com.amadeus.perfgazer.PerfGazerConfig

object ConfigSupport {
  val defaultMaxCacheSize: Int = 10

  implicit class ConfigHelper(c: PerfGazerConfig) {
    def withAllEnabled: PerfGazerConfig =
      c.copy(sqlEnabled = true, jobsEnabled = true, stagesEnabled = true, tasksEnabled = true)

    def withAllDisabled: PerfGazerConfig =
      c.copy(sqlEnabled = false, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)

    def withOnlySqlEnabled: PerfGazerConfig =
      c.copy(sqlEnabled = true, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
  }

  def defaultTestConfig: PerfGazerConfig = PerfGazerConfig(maxCacheSize = defaultMaxCacheSize)
}
