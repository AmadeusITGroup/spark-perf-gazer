package com.amadeus.sparklear

import com.amadeus.sparklear.reports.Report

import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.write

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.ListBuffer

/**
  * Sink of a collection of reports
  */
class JsonSink (
  destination: String = "src/test/json-sink/test.json",
  writeBatchSize: Int = 5,
  debug: Boolean = true
) extends Sink {
  private val reports: ListBuffer[Report] = new ListBuffer[Report]()
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

  override def flush(): Unit = {
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