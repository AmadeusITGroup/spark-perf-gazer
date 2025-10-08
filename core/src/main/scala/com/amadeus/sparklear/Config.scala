package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

import java.time.{LocalDateTime, LocalDate}
import java.time.format.DateTimeFormatter

/** @param sqlEnabled         whether to expose to end-user SQL queries level reports
  * @param jobsEnabled        whether to expose to end-user job level reports
  * @param stagesEnabled      whether to expose to end-user stage level reports
  * @param tasksEnabled       whether to expose to end-user task level reports
  * @param sink               optional method to use as sink for completed [[Report]] instances
  * @param maxCacheSize       maximum amount of elements [[Event]] to keep in memory (per category)
  *                           too large and could cause OOM on the driver, and too small could cause incomplete reports
  *                           generated, so try stay around 200 to 1000 unless you really know what you're doing.
  */
case class Config(
  sqlEnabled: Boolean = true,
  jobsEnabled: Boolean = true,
  stagesEnabled: Boolean = false,
  tasksEnabled: Boolean = false,
  sink: Sink = new LogSink(),
  maxCacheSize: Int
)

case class InputParams(
  displaySparkSqlLevelEvents: Boolean = true,
  displaySparkJobLevelEvents: Boolean = true,
  displaySparkStageLevelEvents: Boolean = true,
  displaySparkTaskLevelEvents: Boolean = false, // too verbose
  listenerEnabled: Boolean = true,
  listenerSinkType: String = "Log",
  listenerOutputBaseDir: String = "/dbfs/tmp/listener/",
  listenerOutputPartitions: Map[String, String] = Map("date" -> LocalDateTime.now().format(DateTimeFormatter.ISO_DATE), "applicationId" -> "spark.app.id")
)

object ConfigBuilder {
  private val MaxInMemoryCacheSize = 100
  private val JsonSinkWriteBatchSize = 100
  private val JsonSinkMaxFileSize = 1L*1024*1024

  def buildConf(sparkConf: Map[String, String], inputParams: InputParams): Config = {
    val listenerBaseDir: String = buildPath(sparkConf, inputParams)

    val sink = inputParams.listenerSinkType match {
      case "Log" => new LogSink()
      case "Json" => new JsonSink(listenerBaseDir, JsonSinkWriteBatchSize, JsonSinkMaxFileSize)
      case _ => new LogSink()
    }

    val conf = Config(
      sqlEnabled = inputParams.displaySparkSqlLevelEvents,
      jobsEnabled = inputParams.displaySparkJobLevelEvents,
      stagesEnabled = inputParams.displaySparkStageLevelEvents,
      tasksEnabled = inputParams.displaySparkTaskLevelEvents,
      sink = sink,
      maxCacheSize = MaxInMemoryCacheSize
    )
    conf
  }

  private def buildPath(
    sparkConf: Map[String, String],
    p: InputParams
  ): String = {
    val sparkConfPattern = "spark\\..*".r
    var listenerOutputPath = if (p.listenerOutputBaseDir.endsWith("/")) p.listenerOutputBaseDir else p.listenerOutputBaseDir + "/"

    p.listenerOutputPartitions.foreach {
      case (key, value) => value match {
        case sparkConfPattern(_*) => listenerOutputPath = listenerOutputPath.withSparkConf(key, value, sparkConf)
        case _ => listenerOutputPath = listenerOutputPath.withPartition(key, value)
      }
    }
    listenerOutputPath
  }

  /**
   * Implicit class that adds path-building methods to String using ad-hoc polymorphism with implicits.
   * Allows fluent API for building filesystem paths with common patterns.
   *
   * Example usage:
   * val basePath = "/tmp/a/"
   * val fullPath = basePath.withDate.withPartition("region", "us-east-1").withPartition("year", "2025")
   * // Results in: "/tmp/a/date=2025-09-23/region=us-east-1/year=2025/"
   */
  implicit class PathOps(val path: String) extends AnyVal {
    def withDate: String = {
      val dateStr = LocalDate.now().format(DateTimeFormatter.ISO_LOCAL_DATE)
      appendPartition("date", dateStr)
    }

    def withDate(formatter: DateTimeFormatter): String = {
      val dateStr = LocalDate.now().format(formatter)
      appendPartition("date", dateStr)
    }

    def withPartition(partitionName: String, value: String): String = {
      appendPartition(partitionName, value)
    }

    def withSparkConf(partitionName: String, tagName: String, sparkConf: Map[String, String]): String = {
      appendPartition(partitionName, sparkConf.getOrElse(tagName, "unknown"))
    }

    def withApplicationId(sparkConf: Map[String, String]): String = {
      appendPartition("applicationId", sparkConf.getOrElse("spark.app.id", "unknown"))
    }

    def withDatabricksTag(partitionName: String, tagName: String, sparkConf: Map[String, String]): String = {
      appendPartition(partitionName, sparkConf.getOrElse(s"spark.databricks.clusterUsageTags.$tagName", "unknown"))
    }

    private def appendPartition(key: String, value: String): String = {
      val cleanPath = if (path.endsWith("/")) path else path + "/"
      val cleanKey = key.replace("=", "_").replace("/", "_")
      val cleanValue = value.replace("=", "_").replace("/", "_")
      cleanPath + s"$cleanKey=$cleanValue/"
    }
  }
}