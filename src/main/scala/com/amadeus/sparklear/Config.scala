package com.amadeus.sparklear

import com.amadeus.sparklear.Config.{DefaultWaitBeforeReadMetricsMs, Sink, SinkStdout}
import com.amadeus.sparklear.converters.{JobJson, JobSerializer, SqlJson, SqlSerializer, StageJson, StageSerializer}
import com.amadeus.sparklear.output.Output
import com.amadeus.sparklear.output.glasses.Glass

case class Config(
  prefix: String = Config.DefaultPrefix,
  showSqls: Boolean = true,
  showJobs: Boolean = true,
  showStages: Boolean = false,
  output: Sink = SinkStdout,
  sqlSerializer: SqlSerializer[_ <: Output] = SqlJson,
  jobSerializer: JobSerializer = JobJson,
  stageSerializer: StageSerializer = StageJson,
  glasses: Seq[Glass] = Seq.empty[Glass],
  waitBeforeReadMetricsMs: Long = DefaultWaitBeforeReadMetricsMs
) {
}

object Config {
  val DefaultPrefix = "SPARKLEAR"
  val DefaultConfig = Config()
  val DefaultWaitBeforeReadMetricsMs = 1000

  sealed trait Sink {
    def output(m: String): Unit
  }
  case object SinkStdout extends Sink {
    override def output(m: String): Unit = System.out.println(m) // scalastyle:ignore
  }
  case object SinkStderr extends Sink {
    override def output(m: String): Unit = System.err.println(m) // scalastyle:ignore
  }
}
