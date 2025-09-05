package com.amadeus.sparklear

import com.amadeus.sparklear.reports._
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.apache.parquet.io.OutputFile
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

/** Sink of a collection of reports to Parquet files.
  *
  * This sink uses Hadoop filesystem interface to write the Parquet files.
  * The output folder structure is built as follows: <destination>/<report-type>.parquet/<uuid>.parquet
  * A typical report path will be "/dbfs/logs/my-app-id/sql-reports.parquet/uuid.parquet" if used from Databricks.
  *
  * @param destination Base directory path where Parquet files will be written, e.g., "/dbfs/logs/my-app-id"
  * @param writeBatchSize Number of reports to accumulate before writing to disk
  */
class ParquetSink(
  destination: String,
  writeBatchSize: Int
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private var reportsCount: Int = 0
  private val sqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val jobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val stageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()
  private val taskReports: ListBuffer[TaskReport] = new ListBuffer[TaskReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  private def getAvroParquetWriter(path: String, schema: Schema) : ParquetWriter[GenericRecord] = {
    val outputPath = new Path(path)
    val outputFile: OutputFile = HadoopOutputFile.fromPath(outputPath, new Configuration())
    AvroParquetWriter
      .builder(outputFile)
      .withSchema(schema)
      .withWriteMode(Mode.OVERWRITE)
      .build()
  }

  // Create Parquet writers
  val sqlReportsDirPath: String = s"$destination/sql-reports.parquet"
  val jobReportsDirPath: String = s"$destination/job-reports.parquet"
  val stageReportsDirPath: String = s"$destination/stage-reports.parquet"
  val taskReportsDirPath: String = s"$destination/task-reports.parquet"

  override def write(report: Report): Unit = {
    reportsCount += 1

    // appends new reports in sink
    report match {
      case sql: SqlReport => sqlReports ++= Seq(sql)
      case job: JobReport => jobReports ++= Seq(job)
      case stage: StageReport => stageReports ++= Seq(stage)
      case task: TaskReport => taskReports ++= Seq(task)
    }

    if ( reportsCount >= writeBatchSize ) {
      logger.debug("ParquetSink Debug : reached writeBatchSize threshold, writing reports ...")
      flush()
      reportsCount = 0
    }
  }

  def flush(): Unit = {
    val uuid: String = java.util.UUID.randomUUID().toString
    val currentSqlReportsPath: String = s"$sqlReportsDirPath/$uuid.parquet"
    val currentJobReportsPath: String = s"$jobReportsDirPath/$uuid.parquet"
    val currentStageReportsPath: String = s"$stageReportsDirPath/$uuid.parquet"
    val currentTaskReportsPath: String = s"$taskReportsDirPath/$uuid.parquet"

    if (sqlReports.nonEmpty) {
      val sqlReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentSqlReportsPath, SqlGenericRecord.reportSchema)
      // Convert all sqlReports to GenericRecords first
      val sqlReportsRecords: Seq[GenericRecord] = sqlReports.map { report =>
        SqlGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", sqlReportsDirPath, sqlReportsRecords.size)
      sqlReportsRecords.foreach(sqlReportsWriter.write)
      sqlReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : sqlReports.clear()")
      sqlReports.clear()
    }
    if (jobReports.nonEmpty) {
      val jobReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentJobReportsPath, JobGenericRecord.reportSchema)
      // Convert all jobReports to GenericRecords first
      val jobReportsRecords: Seq[GenericRecord] = jobReports.map { report =>
        JobGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", jobReportsDirPath, jobReportsRecords.size)
      jobReportsRecords.foreach(jobReportsWriter.write)
      jobReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : jobReports.clear()")
      jobReports.clear()
    }
    if (stageReports.nonEmpty) {
      val stageReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentStageReportsPath, StageGenericRecord.reportSchema)
      // Convert all stageReports to GenericRecords first
      val stageReportsRecords: Seq[GenericRecord] = stageReports.map { report =>
        StageGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", stageReportsDirPath, stageReportsRecords.size)
      stageReportsRecords.foreach(stageReportsWriter.write)
      stageReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : stageReports.clear()")
      stageReports.clear()
    }
    if (taskReports.nonEmpty) {
      val taskReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentTaskReportsPath, TaskGenericRecord.reportSchema)
      // Convert all taskReports to GenericRecords first
      val taskReportsRecords: Seq[GenericRecord] = taskReports.map { report =>
        TaskGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", taskReportsDirPath, taskReportsRecords.size)
      taskReportsRecords.foreach(taskReportsWriter.write)
      taskReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : taskReports.clear()")
      taskReports.clear()
    }
  }

  override def close(): Unit = {
    logger.debug("ParquetSink Debug : flush")
    flush()
  }
}