package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{Report, SqlReport, JobReport, StageReport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.write

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.ListBuffer

/**
  * Sink of a collection of reports
  */
class JsonSink (
  destination: String = "src/test/json-sink",
  writeBatchSize: Int = 5,
  debug: Boolean = true
) extends Sink {
  private var reportsCount: Int = 0
  private val SqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val JobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val StageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
  private val SqlReportsWriter = new PrintWriter(new FileWriter(s"$destination/sql-reports.json", true))
  private val JobReportsWriter = new PrintWriter(new FileWriter(s"$destination/job-reports.json", true))
  private val StageReportsWriter = new PrintWriter(new FileWriter(s"$destination/stage-reports.json", true))

  private def writeReports() = {
    if (SqlReports.nonEmpty) {
      if (debug) {
        println(s"JsonSink Debug : reached writeBatchSize threshold, writing to $destination/sql-reports.json (${SqlReports.size} reports).")
      }
      val json: String = write(SqlReports)
      SqlReportsWriter.println(json)

      // clear reports
      SqlReports.clear()
    }
    if (JobReports.nonEmpty) {
      if (debug) {
        println(s"JsonSink Debug : reached writeBatchSize threshold, writing to $destination/job-reports.json (${JobReports.size} reports).")
      }
      val json: String = write(JobReports)
      JobReportsWriter.println(json)

      // clear reports
      JobReports.clear()
    }
    if (StageReports.nonEmpty) {
      if (debug) {
        println(s"JsonSink Debug : reached writeBatchSize threshold, writing to $destination/stage-reports.json (${StageReports.size} reports).")
      }
      val json: String = write(StageReports)
      StageReportsWriter.println(json)

      // clear reports
      StageReports.clear()
    }
  }

  override def sink(rs: Seq[Report]): Unit = {
    reportsCount += rs.size

    // appends new reports in sink
    rs.map {
      case sql: SqlReport => SqlReports ++= Seq(sql)
      case job: JobReport => JobReports ++= Seq(job)
      case stage: StageReport => StageReports ++= Seq(stage)
    }

    if ( reportsCount >= writeBatchSize ) {
      writeReports()
      reportsCount = 0
    }
  }

  override def flush(): Unit = {
    writeReports()

    // Flush and close writers
    SqlReportsWriter.flush()
    SqlReportsWriter.close()
    JobReportsWriter.flush()
    JobReportsWriter.close()
    StageReportsWriter.flush()
    StageReportsWriter.close()
    if (debug) { println(f"JsonSink Debug : writers closed.") }
  }
}