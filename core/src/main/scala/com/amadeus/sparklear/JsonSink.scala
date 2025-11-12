package com.amadeus.sparklear

import com.amadeus.sparklear.JsonSink._
import com.amadeus.sparklear.reports._
import com.amadeus.sparklear.PathBuilder._
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
  *
  * @param config : object encapsulating destination, writeBatchSize, fileSizeLimit
  */
class JsonSink(val config: JsonSink.Config, sparkConf: SparkConf) extends Sink {
  def this(sparkConf: SparkConf) = {
    this(JsonSink.Config(
      destination = sparkConf.getOption(DestinationKey)
        .getOrElse(throw new IllegalArgumentException(s"Missing required config: $DestinationKey")),
      writeBatchSize = sparkConf.getInt(WriteBatchSizeKey, DefaultWriteBatchSize),
      fileSizeLimit = sparkConf.getLong(FileSizeLimitKey, DefaultFileSizeLimit)
    ), sparkConf)
  }

  val destination: String = config.destination.resolveProperties(sparkConf)

  private val sqlReports: ReportBuffer[SqlReport] =
    new ReportBuffer[SqlReport](config, SqlReportType.name, destination)
  private val jobReports: ReportBuffer[JobReport] =
    new ReportBuffer[JobReport](config, JobReportType.name, destination)
  private val stageReports: ReportBuffer[StageReport] =
    new ReportBuffer[StageReport](config, StageReportType.name, destination)
  private val taskReports: ReportBuffer[TaskReport] =
    new ReportBuffer[TaskReport](config, TaskReportType.name, destination)

  override def write(report: Report): Unit = report match {
    case r: SqlReport => sqlReports.write(r)
    case r: JobReport => jobReports.write(r)
    case r: StageReport => stageReports.write(r)
    case r: TaskReport => taskReports.write(r)
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

    logger.info("JsonSink writers closed.")
  }

  /** String representation of the sink
    */
  override def toString: String = s"JsonSink($config)"

  override def generateViewSnippet(reportType: ReportType): String = {
    JsonViewDDLGenerator.generateViewDDL(destination, reportType.name)
  }
}

object JsonSink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val DestinationKey = "spark.sparklear.sink.json.destination"
  val WriteBatchSizeKey = "spark.sparklear.sink.json.writeBatchSize"
  val FileSizeLimitKey = "spark.sparklear.sink.json.fileSizeLimit"

  val DefaultWriteBatchSize: Int = 100
  val DefaultFileSizeLimit: Long = 200L * 1024 * 1024 // 200 MB

  /** Configuration object for JsonSink
   *
   * @param destination Base directory path where JSON files will be written, e.g., "/dbfs/logs/appid=my-app-id/"
   * @param writeBatchSize Number of reports to accumulate before writing to disk
   * @param fileSizeLimit file size to reach before switching to a new file
   */
  case class Config(
    destination: String,
    writeBatchSize: Int = DefaultWriteBatchSize,
    fileSizeLimit: Long = DefaultFileSizeLimit
  )

  private class ReportBuffer[T <: Report](config: Config, reportType: String, dir: String) {
    implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

    private val folder = new File(dir)
    if (!folder.exists()) folder.mkdirs()

    private var path = s"$dir/$reportType-reports-${Instant.now.toEpochMilli}.json"
    private var writer = new PrintWriter(new FileWriter(path, true))
    private var file = new File(path)

    private val reports: ListBuffer[T] = new ListBuffer[T]()

    private def flushReportsToFile(): Unit = {
      reports.foreach(r => writer.println(asJson(r))) // scalastyle:ignore regex
      // flush writer to write to disk
      writer.flush()
      // clear reports
      reports.clear()
    }

    def write(report: T): Unit = {
      reports += report

      if (reports.size >= config.writeBatchSize) {
        logger.trace("Reached writeBatchSize threshold, writing to {} ({} reports).", file.getPath, reports.size)
        flushReportsToFile()

        if (file.length() >= config.fileSizeLimit) {
          logger.trace("Reached fileSizeLimit threshold, rolling file {} ({} bytes).", file.getPath, file.length())
          writer.close()
          path = s"$dir/$reportType-reports-${Instant.now.toEpochMilli}.json"
          writer = new PrintWriter(new FileWriter(path, true))
          file = new File(path)
          logger.trace("Switched to new file {}.", path)
        }
      }
    }

    def flush(): Unit = {
      if (reports.nonEmpty) {
        logger.trace("Flushing remaining reports to {} ({} reports).", file.getPath, reports.size)
        flushReportsToFile()
      }
    }

    def close(): Unit = {
      writer.close()
    }
  }

  trait JsonViewDDLGenerator {

    protected def runningOnDatabricks: Boolean = {
      sys.env.contains("DATABRICKS_RUNTIME_VERSION")
    }

    /** Generate the CREATE OR REPLACE TEMPORARY VIEW DDL for a partitioned JSON file path.
     *
     * Example of a partitioned path:
     *
     * /tmp/listener/date=2025-09-10/cluster=my-cluster/customer=my-customer/sql-reports-1234.json
     *
     * Rule: the view basePath must be up to (but not including) the first partition-style segment
     * (e.g., date=2025-09-10) starting from which there are ONLY partition style segments.
     * The path should have as many /\* as there are partition segments after the basePath.
     *
     * If the /dbfs mount point is detected (Databricks), it is stripped out.
     *
     * @param destination  The Sink destination directory path where JSON files are written.
     * @param reportName  The report name, that is used as view name too (e.g. "job", "sql", ...).
     */
    def generateViewDDL(destination: String, reportName: String): String = {
      var basePath = destination.extractBasePath
      val starPathPart = destination.extractPartitions.globPathValues
      val fileNameWithWildcard = s"$reportName-reports-*.json"
      if (runningOnDatabricks && basePath.startsWith("/dbfs/")) {
        basePath = basePath.replaceFirst("/dbfs/", "dbfs:/")
      }
      val globPath = s"$basePath$starPathPart".normalizePath + s"$fileNameWithWildcard"
      val ddl =
        s"""|CREATE OR REPLACE TEMPORARY VIEW $reportName
            |USING json
            |OPTIONS (
            |  path \"$globPath\",
            |  basePath \"$basePath\"
            |);""".stripMargin
      ddl
    }
  }

  object JsonViewDDLGenerator extends JsonViewDDLGenerator
}