package com.amadeus.testfwk

import com.amadeus.sparklear.{Sink, JsonSink, ParquetSink}
import com.amadeus.sparklear.reports.Report
import com.amadeus.testfwk.SinkSupport.TestableSink
import scala.collection.mutable.ListBuffer

object SinkSupport {
  // UPDATE make it Seq[Report]
  // class TestableSink(
  //  val reports: ListBuffer[Report] = new ListBuffer[Report](),
  //   stdout: Boolean = false
  // ) extends Sink {
  //   override def sink(i: Report): Unit = {
  //     if (stdout) { println(i) } // scalastyle:ignore regex
  //     reports.+=(i)
  //   }
  // }

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
    val jsonSink = new JsonSink()
    testCode(jsonSink)
  }
  def withParquetSink[T](sparkApplicationId: String,
                         parquetSinkDestination: String,
                         writeBatchSize: Int,
                         debug: Boolean)
                        (testCode: ParquetSink => T): T = {
    val parquetSink = new ParquetSink(
      sparkApplicationId = sparkApplicationId,
      destination = parquetSinkDestination,
      writeBatchSize = writeBatchSize,
      debug = debug)
    testCode(parquetSink)
  }
}
