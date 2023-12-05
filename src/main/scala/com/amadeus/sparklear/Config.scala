package com.amadeus.sparklear

import com.amadeus.sparklear.translators.{
  JobJsonTranslator,
  JobTranslator,
  SqlNodeTranslator,
  SqlTranslator,
  StageJson,
  StageTranslator
}
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.reports.glasses.Glass

case class Config(
  prefix: String = Config.DefaultPrefix,
  showSqls: Boolean = true,
  showJobs: Boolean = true,
  showStages: Boolean = false,
  preReportSink: Option[PreReport => Unit] = None, // for testing purposes
  stringSink: Option[String => Unit] = None,
  outputSink: Option[Report => Unit] = None,
  sqlSerializer: SqlTranslator[_ <: Report] = SqlNodeTranslator,
  jobSerializer: JobTranslator[_ <: Report] = JobJsonTranslator,
  stageSerializer: StageTranslator = StageJson,
  glasses: Seq[Glass] = Seq.empty[Glass],
  maxCacheSize: Int = Config.DefaultCacheSize
)

object Config {
  val DefaultPrefix: String = "SPARKLEAR"
  val DefaultCacheSize: Int = 200
}
