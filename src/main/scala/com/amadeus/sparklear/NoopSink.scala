package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

object NoopSink extends Sink {
  override def sink(r: Report): Unit = { }
}
