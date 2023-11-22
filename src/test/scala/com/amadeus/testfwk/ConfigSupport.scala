package com.amadeus.testfwk

import com.amadeus.sparklear.Config

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(prefix = p)
  }

  def defaultTestConfig: Config = {
    Config.DefaultConfig.withPrefix("")
  }


}
