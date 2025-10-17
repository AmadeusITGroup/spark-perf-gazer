package com.amadeus.sparklear

import com.amadeus.sparklear.JsonSink._
import com.amadeus.sparklear.reports._
import org.apache.spark.SparkConf
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{write => asJson}
import org.json4s.{Formats, NoTypeHints}
import org.slf4j.{Logger, LoggerFactory}

import java.io.{File, FileWriter, PrintWriter}
import java.time.Instant
import scala.collection.mutable.ListBuffer

object JsonSink {
  val DestinationKey = "spark.sparklear.sink.json.destination"
  val WriteBatchSizeKey = "spark.sparklear.sink.json.writeBatchSize"
  val FileSizeLimitKey = "spark.sparklear.sink.json.fileSizeLimit"

  case class Config(
   /** Configuration object for JsonSink
    *
    * @param destination Base directory path where JSON files will be written, e.g., "/dbfs/logs/appid=my-app-id/"
    * @param writeBatchSize Number of reports to accumulate before writing to disk
    * @param fileSizeLimit file size to reach before switching to a new file
    */
    destination: String = "/dbfs/tmp/listener/",
    writeBatchSize: Int = 100,
    fileSizeLimit: Long = 1L * 1024 * 1024
  )

  object JsonViewDDLGenerator {
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
     * @param destination  The Sink destination path where JSON files are written.
     * @param reportName  The report name, that is used as view name too (e.g. "job", "sql", ...).
     */
    def generateViewDDL(destination: String, reportName: String): String = {
      val normalizedDir = destination.replace('\\', '/').stripSuffix("/")
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
}

/** Sink of a collection of reports to JSON files.
  *
  * This sink uses POSIX interface on the driver to write the JSON files.
  * The output folder path is built as follows: <destination>/<report-type>.json
  * A typical report path will be "/dbfs/logs/appid=my-app-id/sql-reports-*.json" if used from Databricks.
  *
  * @param config : object encapsulating destination, writeBatchSize, fileSizeLimit
  */
class JsonSink(val config: JsonSink.Config, sparkConf: SparkConf) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  def this(sparkConf: SparkConf) = {
    this(JsonSink.Config(
      destination = sparkConf.get(DestinationKey, "/dbfs/tmp/listener/"),
      writeBatchSize = sparkConf.getInt(WriteBatchSizeKey, 100),
      fileSizeLimit = sparkConf.getLong(FileSizeLimitKey, 200L*1024*1024)
    ), sparkConf)
  }

  val destination: String = PathBuilder.PathOps(config.destination).resolveProperties(sparkConf)

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

  private val sqlReports: ReportBuffer[SqlReport] = new ReportBuffer[SqlReport]("sql", destination)
  private val jobReports: ReportBuffer[JobReport] = new ReportBuffer[JobReport]("job", destination)
  private val stageReports: ReportBuffer[StageReport] = new ReportBuffer[StageReport]("stage", destination)
  private val taskReports: ReportBuffer[TaskReport] = new ReportBuffer[TaskReport]("task", destination)

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

    import JsonSink._
    Seq("sql", "job", "stage", "task").foreach { reportType =>
      val ddl = JsonViewDDLGenerator.generateViewDDL(config.destination, reportType)
      logger.info(s"To create a temporary view for $reportType reports, run the following DDL:")
      logger.info(ddl)
    }
  }

  /** String representation of the sink
    */
  override def asString: String = s"JsonSink($config)"
}
