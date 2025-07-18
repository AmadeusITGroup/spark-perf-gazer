package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.{write => asJson}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.ListBuffer

/**
  * Sink of a collection of reports
  */
class JsonSink (
  destination: String,
  writeBatchSize: Int = 5
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private var reportsCount: Int = 0
  private val SqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val JobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val StageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)
  private val SqlReportsWriter = new PrintWriter(new FileWriter(s"$destination/sql-reports.json", true))
  private val JobReportsWriter = new PrintWriter(new FileWriter(s"$destination/job-reports.json", true))
  private val StageReportsWriter = new PrintWriter(new FileWriter(s"$destination/stage-reports.json", true))

  override def sink(report: Report): Unit = {
    reportsCount += 1

    // appends new reports in sink
    report match {
      case sql: SqlReport => SqlReports ++= Seq(sql)
      case job: JobReport => JobReports ++= Seq(job)
      case stage: StageReport => StageReports ++= Seq(stage)
    }

    if ( reportsCount >= writeBatchSize ) {
      logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing reports.")
      write()
      reportsCount = 0
    }
  }

  override def write(): Unit = {
    if (SqlReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {}/sql-reports.json ({} reports).", destination, SqlReports.size)
      SqlReports.foreach {r =>
        SqlReportsWriter.println(asJson(r))
      }
      SqlReportsWriter.flush()
      SqlReports.clear()
    }
    if (JobReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {}/job-reports.json ({} reports).", destination, JobReports.size)
      JobReports.foreach {r =>
        JobReportsWriter.println(asJson(r))
      }
      JobReportsWriter.flush()
      JobReports.clear()
    }
    if (StageReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {}/stage-reports.json ({} reports).", destination, StageReports.size)
      StageReports.foreach {r =>
        StageReportsWriter.println(asJson(r))
      }
      StageReportsWriter.flush()
      StageReports.clear()
    }
  }

  override def flush(): Unit = {
    write()

    // Flush and close writers
    SqlReportsWriter.flush()
    SqlReportsWriter.close()
    JobReportsWriter.flush()
    JobReportsWriter.close()
    StageReportsWriter.flush()
    StageReportsWriter.close()

    logger.debug("JsonSink Debug : writers closed.")
  }
}