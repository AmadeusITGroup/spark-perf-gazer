package com.amadeus.testfwk

import com.amadeus.sparklear.{JsonSink, LogSink, Sink}
import com.amadeus.sparklear.reports.{Report, ReportType}
import com.amadeus.testfwk.SinkSupport.TestableSink
import org.apache.spark.SparkConf

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

    override def asString: String = "TestableSink"

    override def generateViewSnippet(reportType: ReportType): String = "No snippet for TestableSink."
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
  def withJsonSink[T](destination: String, writeBatchSize: Int, fileSizeLimit: Long)(testCode: JsonSink => T): T = {
    // build a SparkConf using the JsonSink keys
    val sparkConf = new SparkConf(false)
      .set(JsonSink.DestinationKey, destination)
      .set(JsonSink.WriteBatchSizeKey, writeBatchSize.toString)
      .set(JsonSink.FileSizeLimitKey, fileSizeLimit.toString)

    val jsonSink = new JsonSink(sparkConf)
    testCode(jsonSink)
  }
}
