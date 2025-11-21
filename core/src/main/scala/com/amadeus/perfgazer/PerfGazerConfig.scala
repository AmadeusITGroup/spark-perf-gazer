package com.amadeus.perfgazer

import com.amadeus.perfgazer.PerfGazerConfig.{DefaultJobsEnabled, DefaultMaxCacheSize, DefaultSqlEnabled, DefaultStagesEnabled, DefaultTasksEnabled}
import org.apache.spark.SparkConf

/** @param sqlEnabled         whether to expose to end-user SQL queries level reports
  * @param jobsEnabled        whether to expose to end-user job level reports
  * @param stagesEnabled      whether to expose to end-user stage level reports
  * @param tasksEnabled       whether to expose to end-user task level reports
  * @param maxCacheSize       maximum amount of elements [[Event]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */

case class PerfGazerConfig(
  sqlEnabled: Boolean = DefaultSqlEnabled,
  jobsEnabled: Boolean = DefaultJobsEnabled,
  stagesEnabled: Boolean = DefaultStagesEnabled,
  tasksEnabled: Boolean = DefaultTasksEnabled,
  maxCacheSize: Int = DefaultMaxCacheSize
)

object PerfGazerConfig {
  val SqlEnabledKey = "spark.perfgazer.sql.enabled"
  val JobsEnabledKey = "spark.perfgazer.jobs.enabled"
  val StagesEnabledKey = "spark.perfgazer.stages.enabled"
  val TasksEnabledKey = "spark.perfgazer.tasks.enabled"
  val MaxCacheSizeKey = "spark.perfgazer.max.cache.size"

  val DefaultSqlEnabled: Boolean = true
  val DefaultJobsEnabled: Boolean = true
  val DefaultStagesEnabled: Boolean = true
  val DefaultTasksEnabled: Boolean = false
  val DefaultMaxCacheSize: Int = 100

  def apply(sparkConf: SparkConf): PerfGazerConfig = {
    PerfGazerConfig(
      sqlEnabled = sparkConf.getBoolean(SqlEnabledKey, DefaultSqlEnabled),
      jobsEnabled = sparkConf.getBoolean(JobsEnabledKey, DefaultJobsEnabled),
      stagesEnabled = sparkConf.getBoolean(StagesEnabledKey, DefaultStagesEnabled),
      tasksEnabled = sparkConf.getBoolean(TasksEnabledKey, DefaultTasksEnabled),
      maxCacheSize = sparkConf.getInt(MaxCacheSizeKey, DefaultMaxCacheSize)
    )
  }
}