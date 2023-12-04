package com.amadeus.sparklear

import com.amadeus.sparklear.converters.{JobJson, JobReporter, SqlJson, SqlReporter, StageJson, StageReporter}
import com.amadeus.sparklear.input.Input
import com.amadeus.sparklear.report.Report
import com.amadeus.sparklear.report.glasses.Glass

case class Config(
                   prefix: String = Config.DefaultPrefix,
                   showSqls: Boolean = true,
                   showJobs: Boolean = true,
                   showStages: Boolean = false,
                   inputSink: Option[Input => Unit] = None, // for testing purposes
                   stringSink: Option[String => Unit] = None,
                   outputSink: Option[Report => Unit] = None,
                   sqlSerializer: SqlReporter[_ <: Report] = SqlJson,
                   jobSerializer: JobReporter = JobJson,
                   stageSerializer: StageReporter = StageJson,
                   glasses: Seq[Glass] = Seq.empty[Glass],
                   maxCacheSize: Int = Config.DefaultCacheSize
) {}

object Config {
  val DefaultPrefix: String = "SPARKLEAR"
  val DefaultCacheSize: Int = 200
}
