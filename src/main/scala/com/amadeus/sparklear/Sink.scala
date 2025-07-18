package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

/**
  * Sink of a collection of reports
  */
trait Sink {
  def sink(report: Report): Unit

  def write(): Unit

  def flush(): Unit
}