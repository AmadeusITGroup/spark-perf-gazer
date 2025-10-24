package com.amadeus.sparklear

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

    private def appendPartition(key: String, value: String): String = {
      val cleanPath = if (path.endsWith("/")) path else path + "/"
      val cleanKey = key.replace("=", "_").replace("/", "_")
      val cleanValue = value.replace("=", "_").replace("/", "_")
      cleanPath + s"$cleanKey=$cleanValue/"
    }
  }
}