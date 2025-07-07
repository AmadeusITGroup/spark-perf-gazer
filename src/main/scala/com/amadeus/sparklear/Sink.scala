package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

/**
  * Sink of a collection of reports
  */
trait Sink {
  // def sink(r: Report): Unit
  // UPDATE make it Seq[Report]
  def sink(reports: Seq[Report]): Unit

  def write(): Unit

  def flush(): Unit
}