package com.amadeus.sparklear

import com.amadeus.sparklear.translators.{
  JobJsonTranslator,
  JobTranslator,
  SqlPlanNodeTranslator,
  SqlTranslator,
  StageJson,
  StageTranslator
}
import com.amadeus.sparklear.entities.Entity
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.reports.filters.Filter
import com.amadeus.sparklear.translators.Translator.StringReport

/** @param stringReportPrefix prefix to use when a [[StringReport]] is generated (help end-user discriminate SparklEar in logs)
  * @param showSqls           whether to expose to end-user SQL queries level reports
  * @param showJobs           whether to expose to end-user job level reports
  * @param showStages         whether to expose to end-user stage level reports
  * @param preReportSink      (internal) method to use as sink for completed [[Entity]] instances
  * @param stringReportSink   optional method to use as sink for completed [[Report]] instances (as string)
  * @param reportSink         optional method to use as sink for completed [[Report]] instances
  * @param sqlTranslator      translator to use to generate [[Report]] for SQL queries
  * @param jobTranslator      translator to use to generate [[Report]] for jobs
  * @param stageTranslator    translator to use to generate [[Report]] for stages
  * @param filters            filters to use on [[Report]]
  * @param maxCacheSize       maximum amount of elements [[RawEvent]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */
case class Config(
  stringReportPrefix: String = Config.DefaultStringReportPrefix,
  showSqls: Boolean = true,
  showJobs: Boolean = true,
  showStages: Boolean = false,
  preReportSink: Option[Entity => Unit] = None, // for testing purposes
  stringReportSink: Option[StringReport => Unit] = None,
  reportSink: Option[Report => Unit] = None,
  sqlTranslatorName: String = SqlPlanNodeTranslator.name,
  jobTranslatorName: String = JobJsonTranslator.name,
  stageTranslatorName: String = StageJson.name,
  filters: Seq[Filter] = Seq.empty[Filter],
  maxCacheSize: Int = Config.DefaultCacheSize
) {
  // TODO: use these to avoid collecting (at the source) some objects if won't be used
  //def collectSqls: Boolean = showSqls // collect sqls only if we will show them
  //def collectJobs: Boolean = showJobs // collect jobs only if we will show them
  //def collectStages: Boolean = showStages || showJobs // collect stages only if we will show either stages or jobs

  val sqlTranslator: SqlTranslator[_ <: Report] = SqlTranslator.forName(sqlTranslatorName)
  val jobTranslator: JobTranslator[_ <: Report] = JobTranslator.forName(jobTranslatorName)
  val stageTranslator: StageTranslator = StageTranslator.forName(stageTranslatorName)
}

object Config {
  val DefaultStringReportPrefix: String = "SPARKLEAR "
  val DefaultCacheSize: Int = 200
}
