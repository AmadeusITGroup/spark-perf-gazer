package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{Report, ReportType}

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
    */
  def generateViewSnippet(reportType: ReportType): String

  /** Generate SQL snippets to create views to easily access all report types
    */
  def generateAllViewSnippets(): Seq[String] = {
    ReportType.values.map(r => generateViewSnippet(r))
  }

}
