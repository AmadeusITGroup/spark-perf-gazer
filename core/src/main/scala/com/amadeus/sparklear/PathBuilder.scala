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

    def withDateCustom(pattern: String): String = {
      val dateStr = LocalDate.now().format(DateTimeFormatter.ofPattern(pattern))
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

    private def parseMethods(methods: String): Seq[(String, Seq[Any])] = {
      methods.split(";").toSeq.map { entry =>
        val parts = entry.split(":", 2)
        val method = parts(0).trim
        val args = if (parts.length > 1) parts(1).split(",").map(_.trim).toSeq else Seq.empty
        (method, args)
      }
    }

    def invokePathOpsMethod(method: String, args: Seq[Any] = Seq.empty, sparkConf: Map[String, String] = Map.empty): String = {
      val methodMap: Map[String, Any] = Map(
        "withDate" -> (() => withDate),
        "withDateCustom" -> ((pattern: String) => withDateCustom(pattern)),
        "withPartition" -> ((key: String, value: String) => withPartition(key, value)),
        "withSparkConf" -> ((key: String, tag: String) => withSparkConf(key, tag, sparkConf)),
        "withApplicationId" -> (() => withApplicationId(sparkConf)),
        "withDatabricksTag" -> ((key: String, tag: String) => withDatabricksTag(key, tag, sparkConf)),
        "withDefaultPartitions" -> (() => withDefaultPartitions(sparkConf))
      )

      methodMap.get(method) match {
        case Some(f: (() => String) @unchecked) if args.isEmpty =>
          f()
        case Some(f: (String => String) @unchecked) if args.length == 1 =>
          f(args(0).toString)
        case Some(f: ((String, String) => String) @unchecked) if args.length == 2 =>
          f(args(0).toString, args(1).toString)
        case _ =>
          throw new IllegalArgumentException(s"Invalid method name or arguments for '$method'")
      }
    }

    def invokePathOpsMethods(methods: String, sparkConf: Map[String, String] = Map.empty): String = {
      if (methods.trim.isEmpty) {
        path
      }
      else {
        parseMethods(methods).foldLeft(path) {
          case (currentPath, (method, args)) => currentPath.invokePathOpsMethod(method, args, sparkConf)
        }
      }
    }
  }
}