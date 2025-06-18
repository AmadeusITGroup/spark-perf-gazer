package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

object NoopSink extends Sink {
  // UPDATE make it Seq[Report]
  // override def sink(r: Report): Unit = { }
  def sink(reports: Seq[Report]): Unit = { }

  def finalizeSink(): Unit = { }
}
