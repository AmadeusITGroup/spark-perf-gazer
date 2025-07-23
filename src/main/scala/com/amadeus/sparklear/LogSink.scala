package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report
import org.slf4j.{Logger, LoggerFactory}

import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{write => toJson}


/** Sink that logs reports in json format (info level)
  */
object LogSink extends Sink {
  case class Record(kind: String, report: Report)

  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  override def sink(report: Report): Unit = {
    logger.info(toJson(Record(report.getClass.getName, report))(DefaultFormats))
  }

  override def write(): Unit = {}

  override def flush(): Unit = {}

}
