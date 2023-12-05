package com.amadeus.sparklear

import com.amadeus.sparklear.translators.{
  JobJsonTranslator,
  JobTranslator,
  SqlPlanNodeTranslator,
  SqlTranslator,
  StageJson,
  StageTranslator
}
import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.reports.glasses.Glass
import com.amadeus.sparklear.translators.Translator.StringReport

case class Config(
  stringReportPrefix: String = Config.DefaultStringReportPrefix,
  showSqls: Boolean = true,
  showJobs: Boolean = true,
  showStages: Boolean = false,
  preReportSink: Option[PreReport => Unit] = None, // for testing purposes
  stringReportSink: Option[StringReport => Unit] = None,
  reportSink: Option[Report => Unit] = None,
  sqlTranslator: SqlTranslator[_ <: Report] = SqlPlanNodeTranslator,
  jobTranslator: JobTranslator[_ <: Report] = JobJsonTranslator,
  stageTranslator: StageTranslator = StageJson,
  glasses: Seq[Glass] = Seq.empty[Glass],
  maxCacheSize: Int = Config.DefaultCacheSize
) {
  // TODO: use these to avoid collecting (at the source) some objects if won't be used
  //def collectSqls: Boolean = showSqls // collect sqls only if we will show them
  //def collectJobs: Boolean = showJobs // collect jobs only if we will show them
  //def collectStages: Boolean = showStages || showJobs // collect stages only if we will show either stages or jobs
}

object Config {
  val DefaultStringReportPrefix: String = "SPARKLEAR"
  val DefaultCacheSize: Int = 200
}
