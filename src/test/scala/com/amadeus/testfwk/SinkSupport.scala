package com.amadeus.testfwk

import com.amadeus.sparklear.Sink
import com.amadeus.sparklear.reports.Report
import com.amadeus.testfwk.SinkSupport.TestableSink

import scala.collection.mutable.ListBuffer

object SinkSupport {
  class TestableSink(
    val reports: ListBuffer[Report] = new ListBuffer[Report](),
    stdout: Boolean = false
  ) extends Sink {
    override def sink(i: Report): Unit = {
      if (stdout) { println(i) } // scalastyle:ignore regex
      reports.+=(i)
    }
  }
}
trait SinkSupport {
  def withTestableSink[T](testCode: TestableSink => T): T = {
    val testableSink = new TestableSink()
    testCode(testableSink)
  }
}
