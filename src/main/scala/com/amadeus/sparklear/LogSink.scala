package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report
import org.slf4j.{Logger, LoggerFactory}

/**
  * Sink that logs reports in json format (info level)
  */
object LogSink extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  def sink(r: Report): Unit = {
    logger.info(r.asJson)
  }
}