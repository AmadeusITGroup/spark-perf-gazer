package com.amadeus.testfwk

import com.amadeus.sparklear.{Config, Sink}

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withAllEnabled: Config = c.copy(sqlEnabled = true, jobsEnabled = true, stagesEnabled = true)

    def withAllDisabled: Config = c.copy(sqlEnabled = false, jobsEnabled = false, stagesEnabled = false)
    def withOnlySqlEnabled: Config = c.copy(sqlEnabled = true, jobsEnabled = false, stagesEnabled = false)
    def withSink(s: Sink): Config = c.copy(sink = s)
  }

  def defaultTestConfig: Config = Config()

}
