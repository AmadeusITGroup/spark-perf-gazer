package com.amadeus.sparklear

import com.amadeus.sparklear.converters.{JobJson, JobSerializer, SqlJson, SqlSerializer, StageJson, StageSerializer}
import com.amadeus.sparklear.input.Input
import com.amadeus.sparklear.output.Output
import com.amadeus.sparklear.output.glasses.Glass

case class Config(
  prefix: String = Config.DefaultPrefix,
  showSqls: Boolean = true,
  showJobs: Boolean = true,
  showStages: Boolean = false,
  inputSink: Option[Input => Unit] = None, // for testing purposes
  stringSink: Option[String => Unit] = None,
  outputSink: Option[Output => Unit] = None,
  sqlSerializer: SqlSerializer[_ <: Output] = SqlJson,
  jobSerializer: JobSerializer = JobJson,
  stageSerializer: StageSerializer = StageJson,
  glasses: Seq[Glass] = Seq.empty[Glass],
) {}

object Config {
  val DefaultPrefix = "SPARKLEAR"
}
