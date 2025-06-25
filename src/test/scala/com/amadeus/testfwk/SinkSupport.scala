package com.amadeus.testfwk

import com.amadeus.sparklear.{Sink, JsonSink}
import com.amadeus.sparklear.reports.Report
import com.amadeus.testfwk.SinkSupport.TestableSink
import scala.collection.mutable.ListBuffer

// Imports for JsonSink
import java.io.{FileWriter, PrintWriter}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.NoTypeHints
import org.json4s.Formats

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
}
