package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

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
    */
  def asString: String
}
