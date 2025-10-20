package com.amadeus.sparklear

import com.amadeus.sparklear.SparklearConfig.{DefaultJobsEnabled, DefaultMaxCacheSize, DefaultSqlEnabled, DefaultStagesEnabled, DefaultTasksEnabled}
import org.apache.spark.SparkConf

/** @param sqlEnabled         whether to expose to end-user SQL queries level reports
  * @param jobsEnabled        whether to expose to end-user job level reports
  * @param stagesEnabled      whether to expose to end-user stage level reports
  * @param tasksEnabled       whether to expose to end-user task level reports
  * @param maxCacheSize       maximum amount of elements [[Event]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */

case class SparklearConfig(
  sqlEnabled: Boolean = DefaultSqlEnabled,
  jobsEnabled: Boolean = DefaultJobsEnabled,
  stagesEnabled: Boolean = DefaultStagesEnabled,
  tasksEnabled: Boolean = DefaultTasksEnabled,
  maxCacheSize: Int = DefaultMaxCacheSize
)

object SparklearConfig {
  val DefaultSqlEnabled: Boolean = true
  val DefaultJobsEnabled: Boolean = true
  val DefaultStagesEnabled: Boolean = true
  val DefaultTasksEnabled: Boolean = false
  val DefaultMaxCacheSize: Int = 100

  def apply(sparkConf: SparkConf): SparklearConfig = {
    SparklearConfig(
      sqlEnabled = sparkConf.getBoolean(SparklearSparkConf.SqlEnabledKey, DefaultSqlEnabled),
      jobsEnabled = sparkConf.getBoolean(SparklearSparkConf.JobsEnabledKey, DefaultJobsEnabled),
      stagesEnabled = sparkConf.getBoolean(SparklearSparkConf.StagesEnabledKey, DefaultStagesEnabled),
      tasksEnabled = sparkConf.getBoolean(SparklearSparkConf.TasksEnabledKey, DefaultTasksEnabled),
      maxCacheSize = sparkConf.getInt(SparklearSparkConf.MaxCacheSizeKey, DefaultMaxCacheSize)
    )
  }
}