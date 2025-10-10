package com.amadeus.sparklear


import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter

object PathBuilder {
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

    def withDefaultPartitions(sparkConf: Map[String, String]): String = {
      path.withDate.withApplicationId(sparkConf)
    }

    private def appendPartition(key: String, value: String): String = {
      val cleanPath = if (path.endsWith("/")) path else path + "/"
      val cleanKey = key.replace("=", "_").replace("/", "_")
      val cleanValue = value.replace("=", "_").replace("/", "_")
      cleanPath + s"$cleanKey=$cleanValue/"
    }
  }
}