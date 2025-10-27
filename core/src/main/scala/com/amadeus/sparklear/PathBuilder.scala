package com.amadeus.sparklear

import org.apache.spark.SparkConf

import java.time.LocalDateTime
import java.util.Properties
import scala.util.matching.Regex


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
      appendPartition("date", "${sparklear.now.year}-${sparklear.now.month}-${sparklear.now.day}")
    }

    def withPartition(partitionName: String, value: String): String = {
      appendPartition(partitionName, value)
    }

    def withSparkConf(partitionName: String, tagName: String): String = {
      appendPartition(partitionName, "${" + tagName + "}")
    }

    def withApplicationId: String = {
      appendPartition("applicationId", "${spark.app.id}")
    }

    def withDatabricksTag(partitionName: String, tagName: String): String = {
      appendPartition(partitionName, "${spark.databricks.clusterUsageTags." + tagName + "}")
    }

    def withDefaultPartitions: String = {
      path.withDate.withApplicationId
    }

    def resolveProperties(sparkConf: SparkConf): String = {
      val now = LocalDateTime.now()
      val dateProps = new Properties()
      dateProps.setProperty("sparklear.now.year", now.getYear.toString)
      dateProps.setProperty("sparklear.now.month", f"${now.getMonthValue}%02d")
      dateProps.setProperty("sparklear.now.day", f"${now.getDayOfMonth}%02d")
      dateProps.setProperty("sparklear.now.hour", f"${now.getHour}%02d")
      dateProps.setProperty("sparklear.now.minute", f"${now.getMinute}%02d")

      val placeholderPattern: Regex = """\$\{([^}]+)\}""".r
      val resolved = placeholderPattern.replaceAllIn(path, m =>
        Option(dateProps.getProperty(m.group(1))).orElse(sparkConf.getOption(m.group(1))).getOrElse("unknown")
      )

      resolved
    }

    def withWildcards(): String = {
      val valuePattern: Regex = """=([^/\\]+)""".r
      val resolved = valuePattern.replaceAllIn(path, m => "=*")
      resolved
    }

    def extractBasePath(): String = {
      val partitionsPattern: Regex = """([/\\]+[^=/\\]+=[^/\\]+)+[/\\]*$""".r
      val resolved: String = partitionsPattern.findFirstMatchIn(path) match {
        case Some(m) =>
          path.substring(0, m.start)
        case None =>
          path
      }
      resolved
    }

    def extractPartitions(): String = {
      val partitionsPattern: Regex = """([/\\]+[^=/\\]+=[^/\\]+)+[/\\]*$""".r
      val resolved: String = partitionsPattern.findFirstMatchIn(path) match {
        case Some(m) =>
          path.substring(m.start)
        case None =>
          ""
      }
      resolved
    }

    private def appendPartition(key: String, value: String): String = {
      val cleanPath = if (path.endsWith("/")) path else path + "/"
      val cleanKey = key.replace("=", "_").replace("/", "_")
      val cleanValue = value.replace("=", "_").replace("/", "_")
      cleanPath + s"$cleanKey=$cleanValue/"
    }
  }
}