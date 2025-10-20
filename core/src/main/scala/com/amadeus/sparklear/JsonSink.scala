package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport, TaskReport}
import org.apache.spark.SparkConf
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
  * A typical report path will be "/dbfs/logs/appid=my-app-id/sql-reports-*.json" if used from Databricks.
  */
class JsonSink(sparkConf: SparkConf) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  private val destination: String = sparkConf.get(SparklearSparkConf.JsonSinkDestinationKey, "/dbfs/tmp/listener/")
  private val writeBatchSize: Int = sparkConf.getInt(SparklearSparkConf.JsonSinkWriteBatchSizeKey, 100)
  private val fileSizeLimit: Long = sparkConf.getLong(SparklearSparkConf.JsonSinkFileSizeLimitKey, 1L*1024*1024)

  private case class ReportBuffer[T <: Report](reportType: String, dir: String) {
    private val folder = new File(dir)
    if (!folder.exists()) folder.mkdirs()

    private var path = s"$dir/$reportType-reports-${Instant.now.toEpochMilli}.json"
    private var writer = new PrintWriter(new FileWriter(path, true))
    private var file = new File(path)

    private val reports : ListBuffer[T] = new ListBuffer[T]()

    private def flushReportsToFile(): Unit = {
      reports.foreach(r => writer.println(asJson(r)))  // scalastyle:ignore regex
      // flush writer to write to disk
      writer.flush()
      // clear reports
      reports.clear()
    }

    def write(report: T): Unit = {
      reports += report

      if (reports.size >= writeBatchSize) {
        logger.debug("Reached writeBatchSize threshold, writing to {} ({} reports).", file.getPath, reports.size)
        flushReportsToFile()

        if (file.length() >= fileSizeLimit) {
          logger.debug("Reached fileSizeLimit threshold, rolling file {} ({} bytes).", file.getPath, file.length())
          writer.close()
          path = s"$dir/$reportType-reports-${Instant.now.toEpochMilli}.json"
          writer = new PrintWriter(new FileWriter(path, true))
          file = new File(path)
          logger.debug("Switched to new file {}.", path)
        }
      }
    }

    def flush(): Unit = {
      if (reports.nonEmpty) {
        logger.debug("flushing remaining reports to {} ({} reports).", file.getPath, reports.size)
        flushReportsToFile()
      }
    }

    def close(): Unit = {
      writer.close()
    }
  }

  private val sqlReports: ReportBuffer[SqlReport] = new ReportBuffer[SqlReport]("sql", destination)
  private val jobReports: ReportBuffer[JobReport] = new ReportBuffer[JobReport]("job", destination)
  private val stageReports: ReportBuffer[StageReport] = new ReportBuffer[StageReport]("stage", destination)
  private val taskReports: ReportBuffer[TaskReport] = new ReportBuffer[TaskReport]("task", destination)

  override def write(report: Report): Unit = report match {
    case r: SqlReport   => sqlReports.write(r)
    case r: JobReport   => jobReports.write(r)
    case r: StageReport => stageReports.write(r)
    case r: TaskReport  => taskReports.write(r)
  }

  override def flush(): Unit = {
    sqlReports.flush()
    jobReports.flush()
    stageReports.flush()
    taskReports.flush()
  }

  override def close(): Unit = {
    flush()

    // close writers
    sqlReports.close()
    jobReports.close()
    stageReports.close()
    taskReports.close()

    logger.debug("writers closed.")
  }
}