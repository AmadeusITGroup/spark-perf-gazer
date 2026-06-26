package com.amadeus.perfgazer

import com.amadeus.perfgazer.reports.{Report, ReportType}

/** Sink of a collection of reports
  *
  * Sink implementations are encouraged to provide a constructor that takes a SparkConf as parameter
  * so that they can be instantiated with spark.extraListener configuration
  *
  * Implementations must be thread-safe: methods write and close are
  * invoked by Spark ListenerBus.
  */
trait Sink {

  /** Initialize the sink.
    *
    * Called once when the Spark application starts (from the listener's
    * onApplicationStart callback). Implementations may use this hook to perform
    * eager initialization so that misconfiguration fails fast at application start
    * rather than later during report writing. The default implementation does nothing.
    */
  def init(): Unit = ()

  /** Write a report to the sink
    *
    * @param report Report to write
    */
  def write(report: Report): Unit

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

  /** Report types supported by this sink
    */
  def supportedReportTypes: Set[ReportType]

  /** Generate SQL snippets to create views to easily access all report types
    */
  def generateAllViewSnippets(): Set[String] = {
    supportedReportTypes.map(r => generateViewSnippet(r))
  }
}
