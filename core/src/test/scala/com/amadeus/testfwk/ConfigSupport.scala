package com.amadeus.testfwk

import com.amadeus.perfgazer.PerfGazerConfig

trait ConfigSupport {
  val defaultMaxCacheSize = 10

  implicit class ConfigHelper(c: PerfGazerConfig) {
    def withAllEnabled: PerfGazerConfig = c.copy(sqlEnabled = true, jobsEnabled = true, stagesEnabled = true, tasksEnabled = true)

    def withAllDisabled: PerfGazerConfig = c.copy(sqlEnabled = false, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
    def withOnlySqlEnabled: PerfGazerConfig = c.copy(sqlEnabled = true, jobsEnabled = false, stagesEnabled = false, tasksEnabled = false)
  }

  def defaultTestConfig: PerfGazerConfig = PerfGazerConfig(maxCacheSize = defaultMaxCacheSize)
}
