package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

object NoopSink extends Sink {
  // UPDATE make it Seq[Report]
  // override def sink(r: Report): Unit = { }
  override def sink(reports: Seq[Report]): Unit = { }

  override def write(): Unit = { }

  override def flush(): Unit = { }
}