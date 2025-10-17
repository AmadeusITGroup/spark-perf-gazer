package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report
import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}

/** Sink that logs reports in json format (info level)
  */
class LogSink() extends Sink {
  def this(sparkConf: SparkConf = new SparkConf(false)) = {
    this()
  }

  case class Record(kind: String, report: Report)

  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  override def write(report: Report): Unit = {
    logger.info(toJson(Record(report.getClass.getName, report))(DefaultFormats))
  }

  override def flush(): Unit = {}

  override def close(): Unit = {}

  override def asString: String = "LogSink"

  override def generateViewSnippet(reportType: String): String = "Reports available in the logs."
}
