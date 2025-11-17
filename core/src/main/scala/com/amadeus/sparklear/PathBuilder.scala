package com.amadeus.sparklear

import org.apache.spark.SparkConf

import java.time.LocalDateTime
import java.util.Properties
import scala.util.matching.Regex

object PathBuilder {

  private val SeparatorPattern: Regex = """([/\\]+)""".r
  private val PartitionsPattern: Regex = """([/\\]+[^=/\\]+=[^/\\]+)+[/\\]*$""".r
  private val PlaceholderPattern: Regex = """\$\{([^}]+)\}""".r
  private val ValuePattern: Regex = """=([^/\\]+)""".r

  /** Implicit class that adds path-building methods to String using ad-hoc polymorphism with implicits.
    * Allows fluent API for building filesystem paths with common patterns.
    *
    * Example usage:
    * val basePath = "/tmp/a/"
    * val fullPath = basePath.withDate.withPartition("region", "us-east-1").withPartition("year", "2025")
    * // Results in: "/tmp/a/date=2025-09-23/region=us-east-1/year=2025/"
    */
  implicit class PathOps(val path: String) extends AnyVal {
    private def appendPartition(key: String, value: String): String = {
      val cleanPath = if (path.endsWith("/")) path else path + "/"

      if (key != key.replace("=", "_").replace("/", "_")) {
        throw new IllegalArgumentException(key + " contains invalid characters '=' or '/'")
      }
      if (value != value.replace("=", "_").replace("/", "_")) {
        throw new IllegalArgumentException(value + " contains invalid characters '=' or '/'")
      }

      cleanPath + s"$key=$value/"
    }

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

    /**
      * The normalizePath method is used to standardize the format of a file system path.
      * It ensures that all directory separators in the path are consistent and that the path ends with a separator.
      */
    def normalizePath: String = {
      // Find the first separator used in the path (either / or \)
      val normalized: String = SeparatorPattern.findFirstMatchIn(path) match {
        case Some(m) =>
          val separator: String = m.group(1).take(1)
          // Replace all separators with the first one found, and ensure it ends with a separator
          SeparatorPattern.replaceAllIn(path, separator.replace("\\", "\\\\")).stripSuffix(separator) + separator
        case None =>
          path
      }
      normalized
    }

    /**
      * The resolveProperties method replaces property placeholders in a string path.
      * Placeholders are in the format ${key}.
      * Placeholders are replaced with their corresponding values from a provided map (SparkConf) or internal dateProps map.
      */
    def resolveProperties(sparkConf: SparkConf): String = {
      val now = LocalDateTime.now()
      val dateProps = new Properties()
      dateProps.setProperty("sparklear.now.year", now.getYear.toString)
      dateProps.setProperty("sparklear.now.month", f"${now.getMonthValue}%02d")
      dateProps.setProperty("sparklear.now.day", f"${now.getDayOfMonth}%02d")
      dateProps.setProperty("sparklear.now.hour", f"${now.getHour}%02d")
      dateProps.setProperty("sparklear.now.minute", f"${now.getMinute}%02d")

      val resolved = PlaceholderPattern.replaceAllIn(
        path,
        m => Option(dateProps.getProperty(m.group(1)))
          .orElse(sparkConf.getOption(m.group(1)))
          .getOrElse(throw new IllegalArgumentException(m.group(1) + " is not set"))
      )
      resolved.normalizePath
    }

    /**
      * The globPathValues method is used to replace all partition values in a file system path with a wildcard (*).
      * This is useful for creating a glob pattern to match multiple paths with varying partition values.
      */
    def globPathValues: String = {
      val resolved = ValuePattern.replaceAllIn(path, m => "=*")
      resolved.normalizePath
    }

    /**
      * The extractBasePath method is used to retrieve the base path of a file system path by removing any partition information.
      * Partition information typically follows the format key=value.
      */
    def extractBasePath: String = {
      val resolved: String = PartitionsPattern.findFirstMatchIn(path) match {
        case Some(m) =>
          path.substring(0, m.start)
        case None =>
          path
      }
      resolved.normalizePath
    }

    /**
      * The extractPartitions method is used to retrieve the partition information from a file system path.
      * Partition information typically follows the format key=value.
      */
    def extractPartitions: String = {
      val resolved: String = PartitionsPattern.findFirstMatchIn(path) match {
        case Some(m) =>
          path.substring(m.start)
        case None =>
          ""
      }
      resolved.normalizePath
    }
  }
}
