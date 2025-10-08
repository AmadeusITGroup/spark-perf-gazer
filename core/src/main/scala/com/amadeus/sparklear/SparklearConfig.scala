package com.amadeus.sparklear

/** @param sqlEnabled         whether to expose to end-user SQL queries level reports
  * @param jobsEnabled        whether to expose to end-user job level reports
  * @param stagesEnabled      whether to expose to end-user stage level reports
  * @param tasksEnabled       whether to expose to end-user task level reports
  * @param maxCacheSize       maximum amount of elements [[Event]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */
case class SparklearConfig(
  sqlEnabled: Boolean = true,
  jobsEnabled: Boolean = true,
  stagesEnabled: Boolean = false,
  tasksEnabled: Boolean = false,
  maxCacheSize: Int
)

case class SparklearConfigParams(
  displaySparkSqlLevelEvents: Boolean = true,
  displaySparkJobLevelEvents: Boolean = true,
  displaySparkStageLevelEvents: Boolean = true,
  displaySparkTaskLevelEvents: Boolean = false // too verbose
)

object SparklearConfig {
  private val MaxInMemoryCacheSize = 100

  def build(inputParams: SparklearConfigParams): SparklearConfig = {
    val conf = SparklearConfig(
      sqlEnabled = inputParams.displaySparkSqlLevelEvents,
      jobsEnabled = inputParams.displaySparkJobLevelEvents,
      stagesEnabled = inputParams.displaySparkStageLevelEvents,
      tasksEnabled = inputParams.displaySparkTaskLevelEvents,
      maxCacheSize = MaxInMemoryCacheSize
    )
    conf
  }
}