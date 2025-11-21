package com.amadeus.perfgazer

import com.amadeus.perfgazer.reports.{Report, ReportType}

/** Sink of a collection of reports
  *
  * Sink implementations are encouraged to provide a constructor that takes a SparkConf as parameter
  * so that they can be instantiated with spark.extraListener configuration
  */
trait Sink {

  /** Write a report to the sink
    *
    * @param report Report to write
    */
  def write(report: Report): Unit

  /** Flush any remaining reports
    */
  def flush(): Unit

  /** Close the sink, flushing any remaining reports first
    */
  def close(): Unit

  /** String representation of the sink
    * Used upon sink initialization to log the sink type and configuration.
    */
  def description: String

  /** Generate SQL snippet to create a view to easily access a report of a given type
    */
  def generateViewSnippet(reportType: ReportType): String

  /** Generate SQL snippets to create views to easily access all report types
    */
  def generateAllViewSnippets(): Seq[String] = {
    ReportType.values.map(r => generateViewSnippet(r))
  }
}
