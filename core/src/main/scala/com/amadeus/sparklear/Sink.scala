package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

/** Sink of a collection of reports
  */
trait Sink {

  /** Write a report to the sink
    * @param report Report to write
    */
  def write(report: Report): Unit

  /** Flush any remaining reports
    */
  def flush(): Unit

  /** Close the sink, flushing any remaining reports first
    */
  def close(): Unit

  /** Generate SQL snippet to create a view to easily access a report of a given type
    *
    * TODO: use a type for report type, not string
    */
  def generateViewSnippet(reportType: String): String
}
