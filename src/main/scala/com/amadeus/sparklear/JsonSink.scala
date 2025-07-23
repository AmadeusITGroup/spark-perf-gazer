package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport, TaskReport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.write
import org.slf4j.{Logger, LoggerFactory}

import java.io.{FileWriter, PrintWriter}
import scala.collection.mutable.ListBuffer

/**
  * Sink of a collection of reports
  */
class JsonSink (
  sparkApplicationId: String,
  destination: String,
  writeBatchSize: Int = 5
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  import java.io.File
  val folder = new File(s"$destination/$sparkApplicationId")
  if ( !folder.exists() ) { folder.mkdirs() }

  private var reportsCount: Int = 0
  private val SqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val JobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val StageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()
  private val TaskReports: ListBuffer[TaskReport] = new ListBuffer[TaskReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  // Create Json reports writers
  val SqlReportsPath: String = s"$destination/$sparkApplicationId/sql-reports.json"
  val JobReportsPath: String = s"$destination/$sparkApplicationId/job-reports.json"
  val StageReportsPath: String = s"$destination/$sparkApplicationId/stage-reports.json"
  val TaskReportsPath: String = s"$destination/$sparkApplicationId/task-reports.json"

  private val SqlReportsWriter = new PrintWriter(new FileWriter(SqlReportsPath, true))
  private val JobReportsWriter = new PrintWriter(new FileWriter(JobReportsPath, true))
  private val StageReportsWriter = new PrintWriter(new FileWriter(StageReportsPath, true))
  private val TaskReportsWriter = new PrintWriter(new FileWriter(TaskReportsPath, true))

  override def sink(report: Report): Unit = {
    reportsCount += 1

    // appends new reports in sink
    report match {
      case sql: SqlReport => SqlReports ++= Seq(sql)
      case job: JobReport => JobReports ++= Seq(job)
      case stage: StageReport => StageReports ++= Seq(stage)
      case task: TaskReport => TaskReports ++= Seq(task)
    }

    if ( reportsCount >= writeBatchSize ) {
      logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing reports ...")
      write()
      reportsCount = 0
    }
  }

  override def write(): Unit = {
    if (SqlReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", SqlReportsPath, SqlReports.size)
      SqlReports.foreach { r =>
        val json: String = org.json4s.jackson.Serialization.write(r)
        SqlReportsWriter.println(json) // scalastyle:ignore regex
      }
      SqlReportsWriter.flush()

      // clear reports
      SqlReports.clear()
    }
    if (JobReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", JobReportsPath, JobReports.size)
      JobReports.foreach { r =>
        val json: String = org.json4s.jackson.Serialization.write(r)
        JobReportsWriter.println(json) // scalastyle:ignore regex
      }
      JobReportsWriter.flush()

      // clear reports
      JobReports.clear()
    }
    if (StageReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", StageReportsPath, StageReports.size)
      StageReports.foreach { r =>
        val json: String = org.json4s.jackson.Serialization.write(r)
        StageReportsWriter.println(json) // scalastyle:ignore regex
      }
      StageReportsWriter.flush()

      // clear reports
      StageReports.clear()
    }
    if (TaskReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", TaskReportsPath, TaskReports.size)
      TaskReports.foreach { r =>
        val json: String = org.json4s.jackson.Serialization.write(r)
        TaskReportsWriter.println(json) // scalastyle:ignore regex
      }
      TaskReportsWriter.flush()

      // clear reports
      TaskReports.clear()
    }
  }

  override def flush(): Unit = {
    write()

    // Flush and close writers
    SqlReportsWriter.close()
    JobReportsWriter.close()
    StageReportsWriter.close()
    TaskReportsWriter.close()

    logger.debug("JsonSink Debug : writers closed.")
  }
}