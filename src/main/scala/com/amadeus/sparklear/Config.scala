package com.amadeus.sparklear

import com.amadeus.sparklear.Config.{DefaultPrefix, Output, OutputStdout}
import com.amadeus.sparklear.reports.converters.{JobJson, JobSerializer, SqlJson, SqlSerializer, StageJson, StageSerializer}

case class Config(
  prefix: String = DefaultPrefix,
  reportStageDetails: Boolean = false,
  output: Output = OutputStdout,
  sqlSerializer: SqlSerializer = SqlJson,
  jobSerializer: JobSerializer = JobJson,
  stageSerializer: StageSerializer = StageJson
)

object Config {
  val DefaultPrefix = "SPARKLEAR"
  val DefaultConfig = Config()

  sealed trait Output {
    def output(m: String): Unit
  }
  case object OutputStdout extends Output {
    override def output(m: String): Unit = System.out.println(m) // scalastyle:ignore
  }
  case object OutputStderr extends Output {
    override def output(m: String): Unit = System.err.println(m) // scalastyle:ignore
  }
}
