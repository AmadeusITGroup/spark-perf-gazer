package com.amadeus.sparklear

import com.amadeus.sparklear.JsonSink.JsonViewDDLGenerator
import com.amadeus.sparklear.reports._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write => asJson}
import org.json4s.{Formats, NoTypeHints}
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
class JsonSink(
  val config: JsonSinkConfig
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  private case class ReportBuffer[T <: Report](reportType: String, dir: String) {
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
        logger.debug("Reached writeBatchSize threshold, writing to {} ({} reports).", file.getPath, reports.size)
        flushReportsToFile()

        if (file.length() >= config.fileSizeLimit) {
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

  private val sqlReports: ReportBuffer[SqlReport] =
    new ReportBuffer[SqlReport](SqlReportType.name, config.destination)
  private val jobReports: ReportBuffer[JobReport] =
    new ReportBuffer[JobReport](JobReportType.name, config.destination)
  private val stageReports: ReportBuffer[StageReport] =
    new ReportBuffer[StageReport](StageReportType.name, config.destination)
  private val taskReports: ReportBuffer[TaskReport] =
    new ReportBuffer[TaskReport](TaskReportType.name, config.destination)

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

    logger.debug("writers closed.")
  }

  override def generateViewSnippet(reportType: ReportType): String = {
    JsonViewDDLGenerator.generateViewDDL(config.destination, reportType.name)
  }
}

object JsonSink {

  trait JsonViewDDLGenerator {

    protected def runningOnDatabricks: Boolean = {
      sys.env.contains("DATABRICKS_RUNTIME_VERSION")
    }

    private def normalizeDir(path: String): String = {
      val normalized = path
        .replace('\\', '/')
        .stripSuffix("/")
      if (runningOnDatabricks && normalized.startsWith("/dbfs")) {
        normalized.replaceFirst("^/dbfs", "dbfs:")
      } else {
        normalized
      }
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
      val normalizedDir = normalizeDir(destination)
      val segments = normalizedDir.split('/').filter(_.nonEmpty).toList
      def isPartition(s: String) = s.contains('=')
      // Find the first index where all remaining segments are partition-style
      val baseIdx = segments.indices
        .find { idx =>
          val rest = segments.drop(idx)
          rest.nonEmpty && rest.forall(isPartition)
        }
        .getOrElse(segments.length)
      val leadingSlash = if (normalizedDir.startsWith("/")) "/" else ""
      val basePath =
        if (baseIdx == 0) {
          leadingSlash
        } else {
          s"${leadingSlash}${segments.take(baseIdx).mkString("/")}/"
        }
      val partitionCount = segments.length - baseIdx
      val starPathPart = if (partitionCount == 0) "" else List.fill(partitionCount)("*").mkString("", "/", "/")
      val fileNameWithWildcard = s"${reportName}-reports-*.json"
      val globPath = s"$basePath$starPathPart$fileNameWithWildcard"
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
