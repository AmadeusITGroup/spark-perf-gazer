package com.amadeus.perfgazer

import com.amadeus.perfgazer.JsonSink._
import com.amadeus.perfgazer.reports._
import com.amadeus.perfgazer.PathBuilder._
import org.apache.spark.SparkConf
import org.slf4j.{Logger, LoggerFactory}

/** Sink of a collection of reports to JSON files.
  *
  * This sink uses POSIX interface on the driver to write the JSON files.
  * The output folder path is built as follows: <destination>/<report-type>.json
  * A typical report path will be "/dbfs/logs/appid=my-app-id/sql-reports-*.json" if used from Databricks.
  *
  * @param config : object encapsulating destination, writeBatchSize, fileSizeLimit
  */
class JsonSink(
  val config: JsonSink.Config,
  sparkConf: SparkConf,
  reportTypes: Set[ReportType] = ReportType.standardTypes
) extends Sink {

  def this(sparkConf: SparkConf) = {
    this(
      JsonSink.Config(
        destination = sparkConf
          .getOption(DestinationKey)
          .getOrElse(throw new IllegalArgumentException(s"Missing required config: $DestinationKey")),
        writeBatchSize = sparkConf.getInt(WriteBatchSizeKey, DefaultWriteBatchSize),
        fileSizeLimit = sparkConf.getLong(FileSizeLimitKey, DefaultFileSizeLimit),
        asyncFlushTimeoutMillisecs = sparkConf.getLong(AsyncFlushTimeoutMillisecsKey, DefaultAsyncFlushTimeoutMillisecs),
        waitForGracefulCloseMillisecs = sparkConf.getLong(WaitForCloseTimeoutMillisecsKey, DefaultWaitForGracefulCloseMillisecs)
      ),
      sparkConf
    )
  }

  override def supportedReportTypes: Set[ReportType] = reportTypes

  val destination: String = config.destination.resolveProperties(sparkConf)

  val queues: Set[ReportWriter] = supportedReportTypes.map(
    new ReportWriter(config, _, destination)
  )

  override def write(r: Report): Unit =
    queues.find(q => q.reportType == r.reportType).foreach(_.write(r))

  override def close(): Unit = {
    // close writers
    queues.foreach(_.close())
    logger.info("JsonSink writers closed.")
  }

  /** String representation of the sink
    * Used upon sink initialization to log the sink type and configuration.
    */
  override def description: String = s"JsonSink($config)"

  override def generateViewSnippet(reportType: ReportType): String = {
    JsonViewDDLGenerator.generateViewDDL(destination, reportType.name)
  }
}

object JsonSink {
  val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  val DestinationKey = "spark.perfgazer.sink.json.destination"
  val WriteBatchSizeKey = "spark.perfgazer.sink.json.writeBatchSize"
  val FileSizeLimitKey = "spark.perfgazer.sink.json.fileSizeLimit"
  val AsyncFlushTimeoutMillisecsKey = "spark.perfgazer.sink.json.asyncFlushTimeoutMillisecsKey"
  val WaitForCloseTimeoutMillisecsKey = "spark.perfgazer.sink.json.waitForCloseTimeoutMillisecsKey"

  val DefaultWriteBatchSize: Int = 100
  val DefaultFileSizeLimit: Long = 200L * 1024 * 1024 // 200 MB
  val DefaultAsyncFlushTimeoutMillisecs: Long = 10 * 1000L
  val DefaultWaitForGracefulCloseMillisecs: Long = 10 * 1000L

  /** Configuration object for JsonSink
    *
    * @param destination Base directory path where JSON files will be written, e.g., "/dbfs/logs/appid=my-app-id/"
    * @param writeBatchSize Number of reports to accumulate before writing to disk
    * @param fileSizeLimit file size to reach before switching to a new file (in bytes)
    * @param asyncFlushTimeoutMillisecs Maximum time to wait regularly before flushing reports to disk (in milliseconds)
    * @param waitForGracefulCloseMillisecs Maximum time to wait for graceful close of the sink (in milliseconds)
    */
  case class Config(
     destination: String,
     writeBatchSize: Int = DefaultWriteBatchSize,
     fileSizeLimit: Long = DefaultFileSizeLimit,
     asyncFlushTimeoutMillisecs: Long = DefaultAsyncFlushTimeoutMillisecs,
     waitForGracefulCloseMillisecs: Long = DefaultWaitForGracefulCloseMillisecs
  )

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
