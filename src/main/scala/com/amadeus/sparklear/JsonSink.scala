package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport, TaskReport}
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
  sparkApplicationId: String,
  destination: String,
  writeBatchSize: Int = 5
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  import java.io.File
  val folder = new File(s"$destination/$sparkApplicationId")
  if ( !folder.exists() ) { folder.mkdirs() }

  private var reportsCount: Int = 0
  private val sqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val jobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val stageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()
  private val taskReports: ListBuffer[TaskReport] = new ListBuffer[TaskReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  // Create Json reports writers
  val sqlReportsPath: String = s"$destination/$sparkApplicationId/sql-reports.json"
  val jobReportsPath: String = s"$destination/$sparkApplicationId/job-reports.json"
  val stageReportsPath: String = s"$destination/$sparkApplicationId/stage-reports.json"
  val taskReportsPath: String = s"$destination/$sparkApplicationId/task-reports.json"

  private val sqlReportsWriter = new PrintWriter(new FileWriter(sqlReportsPath, true))
  private val jobReportsWriter = new PrintWriter(new FileWriter(jobReportsPath, true))
  private val stageReportsWriter = new PrintWriter(new FileWriter(stageReportsPath, true))
  private val taskReportsWriter = new PrintWriter(new FileWriter(taskReportsPath, true))

  override def sink(report: Report): Unit = {
    reportsCount += 1

    // appends new reports in sink
    report match {
      case sql: SqlReport => sqlReports ++= Seq(sql)
      case job: JobReport => jobReports ++= Seq(job)
      case stage: StageReport => stageReports ++= Seq(stage)
      case task: TaskReport => taskReports ++= Seq(task)
    }

    if ( reportsCount >= writeBatchSize ) {
      logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing reports ...")
      write()
      reportsCount = 0
    }
  }

  override def write(): Unit = {
    if (sqlReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", sqlReportsPath, sqlReports.size)
      sqlReports.foreach { r =>
        sqlReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      sqlReportsWriter.flush()
      // clear reports
      sqlReports.clear()
    }
    if (jobReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", jobReportsPath, jobReports.size)
      jobReports.foreach { r =>
        jobReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      jobReportsWriter.flush()
      // clear reports
      jobReports.clear()
    }
    if (stageReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", stageReportsPath, stageReports.size)
      stageReports.foreach { r =>
        stageReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      stageReportsWriter.flush()
      // clear reports
      stageReports.clear()
    }
    if (taskReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", taskReportsPath, taskReports.size)
      taskReports.foreach { r =>
        taskReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      taskReportsWriter.flush()
      // clear reports
      taskReports.clear()
    }
  }

  override def flush(): Unit = {
    write()

    // close writers
    sqlReportsWriter.close()
    jobReportsWriter.close()
    stageReportsWriter.close()
    taskReportsWriter.close()

    logger.debug("JsonSink Debug : writers closed.")
  }
}