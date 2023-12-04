package com.amadeus.testfwk

import com.amadeus.sparklear.Config
import com.amadeus.sparklear.converters.{JobSerializer, SqlSerializer, StageSerializer}
import com.amadeus.sparklear.input.Input
import com.amadeus.sparklear.output.Output
import com.amadeus.sparklear.output.glasses.{Glass, SqlNodeGlass}

trait ConfigSupport {

  implicit class ConfigHelper(c: Config) {
    def withPrefix(p: String): Config = c.copy(prefix = p)
    def withSqlSerializer(s: SqlSerializer[_ <: Output]): Config = c.copy(sqlSerializer = s)
    def withJobSerializer(s: JobSerializer): Config = c.copy(jobSerializer = s)
    def withStageSerializer(s: StageSerializer): Config = c.copy(stageSerializer = s)
    def withAllEnabled: Config = c.copy(showSqls = true, showJobs = true, showStages = true)
    def withGlasses(g: Seq[Glass]): Config = c.copy(glasses = g)
    def withStringSink(ss: String => Unit): Config = c.copy(stringSink = Some(ss))
    def withOutputSink(ss: Output => Unit): Config = c.copy(outputSink = Some(ss))
    def withInputSink(ss: Input => Unit): Config = c.copy(inputSink = Some(ss))
  }

  def defaultTestConfig: Config = {
    Config().withPrefix("")
  }


}
