package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

/**
  * Sink of a collection of reports
  */
trait Sink {
  def sink(r: Report): Unit // TODO make it Seq[Report]
}