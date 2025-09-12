package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport, TaskReport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization.{write => asJson}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import scala.collection.mutable.ListBuffer

/** Sink of a collection of reports to JSON files.
  *
  * This sink uses POSIX interface on the driver to write the JSON files.
  * The output folder path is built as follows: <destination>/<report-type>.json
  * A typical report path will be "/dbfs/logs/my-app-id/sql-reports.json" if used from Databricks.
  *
  * @param destination Base directory path where JSON files will be written, e.g., "/dbfs/logs/my-app-id/"
  * @param writeBatchSize Number of reports to accumulate before writing to disk
  */
class JsonSink(
  destination: String,
  writeBatchSize: Int,
  fileSizeLimit: Long
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val sqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val jobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val stageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()
  private val taskReports: ListBuffer[TaskReport] = new ListBuffer[TaskReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  // Create Json reports folders
  val sqlReportsDir: String = s"$destination/level=sql/"
  val jobReportsDir: String = s"$destination/level=job/"
  val stageReportsDir: String = s"$destination/level=stage/"
  val taskReportsDir: String = s"$destination/level=task/"

  Seq(sqlReportsDir, jobReportsDir, stageReportsDir, taskReportsDir).foreach{ dir =>
    val folder = new File(s"$dir")
    if (!folder.exists()) { folder.mkdirs }
  }

  // Init Json reports writers
  private var sqlReportsPath: String = s"$sqlReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
  private var jobReportsPath: String = s"$jobReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
  private var stageReportsPath: String = s"$stageReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
  private var taskReportsPath: String = s"$taskReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"

  private var sqlReportsWriter = new PrintWriter(new FileWriter(sqlReportsPath, true))
  private var jobReportsWriter = new PrintWriter(new FileWriter(jobReportsPath, true))
  private var stageReportsWriter = new PrintWriter(new FileWriter(stageReportsPath, true))
  private var taskReportsWriter = new PrintWriter(new FileWriter(taskReportsPath, true))

  private var sqlReportsFile = new File(sqlReportsPath)
  private var jobReportsFile = new File(jobReportsPath)
  private var stageReportsFile = new File(stageReportsPath)
  private var taskReportsFile = new File(taskReportsPath)

  override def write(report: Report): Unit = {
    // appends new reports in sink
    report match {
      case sql: SqlReport =>
        sqlReports ++= Seq(sql)

        if (sqlReports.size >= writeBatchSize) {
          logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing to {} ({} reports.", sqlReportsPath, sqlReports.size)
          sqlReports.foreach { r =>
            sqlReportsWriter.println(asJson(r)) // scalastyle:ignore regex
          }
          // flush writer to write to disk
          sqlReportsWriter.flush()
          // clear reports
          sqlReports.clear()

          // File rolling
          if (sqlReportsFile.length() >= fileSizeLimit) {
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, closing current file {} ({}).", sqlReportsPath, sqlReportsFile.length())
            sqlReportsWriter.close()
            sqlReportsPath = s"$sqlReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, switching to new file {}.", sqlReportsPath)
            sqlReportsWriter = new PrintWriter(new FileWriter(sqlReportsPath, true))
            sqlReportsFile = new File(sqlReportsPath)
          }
        }
      case job: JobReport =>
        jobReports ++= Seq(job)

        if (jobReports.size >= writeBatchSize) {
          logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing to {} ({} reports.", jobReportsPath, jobReports.size)
          jobReports.foreach { r =>
            jobReportsWriter.println(asJson(r)) // scalastyle:ignore regex
          }
          // flush writer to write to disk
          jobReportsWriter.flush()
          // clear reports
          jobReports.clear()

          // File rolling
          if (jobReportsFile.length() >= fileSizeLimit) {
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, closing current file {} ({}).", jobReportsPath, jobReportsFile.length())
            jobReportsWriter.close()
            jobReportsPath = s"$jobReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, switching to new file {}.", jobReportsPath)
            jobReportsWriter = new PrintWriter(new FileWriter(jobReportsPath, true))
            jobReportsFile = new File(jobReportsPath)
          }
        }
      case stage: StageReport =>
        stageReports ++= Seq(stage)

        if (stageReports.size >= writeBatchSize) {
          logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing to {} ({} reports.", stageReportsPath, stageReports.size)
          stageReports.foreach { r =>
            stageReportsWriter.println(asJson(r)) // scalastyle:ignore regex
          }
          // flush writer to write to disk
          stageReportsWriter.flush()
          // clear reports
          stageReports.clear()

          // File rolling
          if (stageReportsFile.length() >= fileSizeLimit) {
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, closing current file {} ({}).", stageReportsPath, stageReportsFile.length())
            stageReportsWriter.close()
            stageReportsPath = s"$stageReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, switching to new file {}.", stageReportsPath)
            stageReportsWriter = new PrintWriter(new FileWriter(stageReportsPath, true))
            stageReportsFile = new File(stageReportsPath)
          }
        }
      case task: TaskReport =>
        taskReports ++= Seq(task)

        if (taskReports.size >= writeBatchSize) {
          logger.debug("JsonSink Debug : reached writeBatchSize threshold, writing to {} ({} reports)", taskReportsPath, taskReports.size)
          taskReports.foreach { r =>
            taskReportsWriter.println(asJson(r)) // scalastyle:ignore regex
          }
          // flush writer to write to disk
          taskReportsWriter.flush()
          // clear reports
          taskReports.clear()

          // File rolling
          if (taskReportsFile.length() >= fileSizeLimit) {
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, closing current file {} ({}).", taskReportsPath, taskReportsFile.length())
            taskReportsWriter.close()
            taskReportsPath = s"$taskReportsDir/reports-${Instant.now.toEpochMilli.toString}.json"
            logger.debug("JsonSink Debug : reached fileSizeLimit threshold, switching to new file {}.", taskReportsPath)
            taskReportsWriter = new PrintWriter(new FileWriter(taskReportsPath, true))
            taskReportsFile = new File(taskReportsPath)
          }
        }
    }
  }

  override def flush(): Unit = {
    if (sqlReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", sqlReportsFile, sqlReports.size)
      sqlReports.foreach { r =>
        sqlReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      sqlReportsWriter.flush()
      // clear reports
      sqlReports.clear()
    }
    if (jobReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", jobReportsFile, jobReports.size)
      jobReports.foreach { r =>
        jobReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      jobReportsWriter.flush()
      // clear reports
      jobReports.clear()
    }
    if (stageReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", stageReportsFile, stageReports.size)
      stageReports.foreach { r =>
        stageReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      stageReportsWriter.flush()
      // clear reports
      stageReports.clear()
    }
    if (taskReports.nonEmpty) {
      logger.debug("JsonSink Debug : writing to {} ({} reports).", taskReportsFile, taskReports.size)
      taskReports.foreach { r =>
        taskReportsWriter.println(asJson(r)) // scalastyle:ignore regex
      }
      // flush writer to write to disk
      taskReportsWriter.flush()
      // clear reports
      taskReports.clear()
    }
  }

  override def close(): Unit = {
    flush()

    // close writers
    sqlReportsWriter.close()
    jobReportsWriter.close()
    stageReportsWriter.close()
    taskReportsWriter.close()

    logger.debug("JsonSink Debug : writers closed.")
  }
}
