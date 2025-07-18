package com.amadeus.testfwk

import com.amadeus.sparklear.{Sink, JsonSink, ParquetSink}
import com.amadeus.sparklear.reports.Report
import com.amadeus.testfwk.SinkSupport.TestableSink
import scala.collection.mutable.ListBuffer

object SinkSupport {
  class TestableSink(
    val reports: ListBuffer[Report] = new ListBuffer[Report](),
    stdout: Boolean = false
  ) extends Sink {
    override def sink(rs: Seq[Report]): Unit = {
      if (stdout) { rs.foreach(println) } // scalastyle:ignore regex
      reports ++= rs
    }

    override def write(): Unit = { }

    override def flush(): Unit = { }
  }
}

trait SinkSupport {
  def withTestableSink[T](testCode: TestableSink => T): T = {
    val testableSink = new TestableSink()
    testCode(testableSink)
  }
  def withJsonSink[T](testCode: JsonSink => T): T = {
    val jsonSink = new JsonSink(
      destination = "src/test/json-sink"
    )
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
