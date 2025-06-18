package com.amadeus.testfwk

import com.amadeus.sparklear.Sink
import com.amadeus.sparklear.reports.Report
import com.amadeus.testfwk.SinkSupport.{TestableSink, JsonSink}
import scala.collection.mutable.ListBuffer

// Imports for JsonSink
import java.io.{FileWriter, PrintWriter}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.NoTypeHints
import org.json4s.Formats

object SinkSupport {
  // UPDATE make it Seq[Report]
  /*
  class TestableSink(
    val reports: ListBuffer[Report] = new ListBuffer[Report](),
    stdout: Boolean = false
  ) extends Sink {
    override def sink(i: Report): Unit = {
      if (stdout) { println(i) } // scalastyle:ignore regex
      reports.+=(i)
    }
  }
  */
  class TestableSink(
    val reports: ListBuffer[Report] = new ListBuffer[Report](),
    stdout: Boolean = false
  ) extends Sink {
    override def sink(rs: Seq[Report]): Unit = {
      if (stdout) { rs.foreach(println) } // scalastyle:ignore regex
      reports ++= rs
    }

    def finalizeSink(): Unit = { }
  }

  class JsonSink(
    val reports: ListBuffer[Report] = new ListBuffer[Report](),
    destination: String = "src/test/json-sink/test.json",
    writeBatchSize: Int = 5,
    debug: Boolean = true
  ) extends Sink {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
    private val writer = new PrintWriter(new FileWriter(destination, true))

    override def sink(rs: Seq[Report]): Unit = {
      // appends new reports in sink
      reports ++= rs

      if ( reports.size >= writeBatchSize ) {
        // write reports in destination
        if (debug) { println(s"JsonSink Debug : reached writeBatchSize threshold, writing to $destination (${reports.size} reports).") }
        val json: String = write(reports)
        writer.println(json)

        // clear reports
        reports.clear()
      }
    }

    def finalizeSink(): Unit = {
      if ( reports.nonEmpty ) {
        // write reports in destination
        if (debug) { println(s"JsonSink Debug : finalizing sink, writing to $destination (${reports.size} reports).") }
        val json: String = write(reports)
        writer.println(json)

        // clear reports
        reports.clear()
      }
      // Flush and close writer
      writer.flush()
      writer.close()
      if (debug) { println(f"JsonSink Debug : writer closed.") }
    }
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
