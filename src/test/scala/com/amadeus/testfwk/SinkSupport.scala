package com.amadeus.testfwk

import com.amadeus.sparklear.{JsonSink, ParquetSink, LogSink, Sink}
import com.amadeus.sparklear.reports.Report
import com.amadeus.testfwk.SinkSupport.TestableSink
import scala.collection.mutable.ListBuffer

object SinkSupport {
  class TestableSink(
    val reports: ListBuffer[Report] = new ListBuffer[Report](),
    stdout: Boolean = false
  ) extends Sink {
    override def write(report: Report): Unit = {
      if (stdout) { println(report) } // scalastyle:ignore regex
      reports += report
    }

    override def flush(): Unit = {}

    override def close(): Unit = {}
  }
}

trait SinkSupport {
  def withTestableSink[T](testCode: TestableSink => T): T = {
    val testableSink = new TestableSink()
    testCode(testableSink)
  }
  def withLogSink[T](testCode: LogSink => T): T = {
    val logSink = new LogSink()
    testCode(logSink)
  }
  def withJsonSink[T](sparkApplicationId: String,
                      parquetSinkDestination: String,
                      writeBatchSize: Int)
                     (testCode: JsonSink => T): T = {
    val jsonSink = new JsonSink(
      sparkApplicationId = sparkApplicationId,
      destination = parquetSinkDestination,
      writeBatchSize = writeBatchSize)
    testCode(jsonSink)
  }
  def withParquetSink[T](sparkApplicationId: String,
                         parquetSinkDestination: String,
                         writeBatchSize: Int)
                        (testCode: ParquetSink => T): T = {
    val parquetSink = new ParquetSink(
      sparkApplicationId = sparkApplicationId,
      destination = parquetSinkDestination,
      writeBatchSize = writeBatchSize)
    testCode(parquetSink)
  }
}
