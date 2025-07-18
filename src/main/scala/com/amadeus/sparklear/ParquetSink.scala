package com.amadeus.sparklear

import com.amadeus.sparklear.LogSink.getClass
import com.amadeus.sparklear.reports.{JobGenericRecord, JobReport, Report, SqlGenericRecord, SqlReport, StageGenericRecord, StageReport, TaskGenericRecord, TaskReport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.collection.mutable.ListBuffer
import org.apache.parquet.io.OutputFile
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.conf.Configuration
import org.apache.parquet.avro.AvroParquetWriter
import org.apache.parquet.hadoop.ParquetWriter
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetFileWriter.Mode
import org.apache.parquet.hadoop.util.HadoopOutputFile
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/**
  * Sink of a collection of reports
  */
class ParquetSink(
  sparkApplicationId: String,
  destination: String,
  writeBatchSize: Int = 5
) extends Sink {
  implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private var reportsCount: Int = 0
  private val SqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val JobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val StageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()
  private val TaskReports: ListBuffer[TaskReport] = new ListBuffer[TaskReport]()

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
  val SqlReportsPath: String = s"$destination/$sparkApplicationId/sql-reports.parquet"
  val JobReportsPath: String = s"$destination/$sparkApplicationId/job-reports.parquet"
  val StageReportsPath: String = s"$destination/$sparkApplicationId/stage-reports.parquet"
  val TaskReportsPath: String = s"$destination/$sparkApplicationId/task-reports.parquet"

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
      logger.debug("ParquetSink Debug : reached writeBatchSize threshold, writing reports ...")
      write()
      reportsCount = 0
    }
  }

  def write(): Unit = {
    val uuid: String = java.util.UUID.randomUUID().toString
    val currentSqlReportsPath: String = s"$SqlReportsPath/$uuid.parquet"
    val currentJobReportsPath: String = s"$JobReportsPath/$uuid.parquet"
    val currentStageReportsPath: String = s"$StageReportsPath/$uuid.parquet"
    val currentTaskReportsPath: String = s"$TaskReportsPath/$uuid.parquet"

    if (SqlReports.nonEmpty) {
      val SqlReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentSqlReportsPath, SqlGenericRecord.reportSchema)
      // Convert all SqlReports to GenericRecords first
      val SqlReportsRecords: Seq[GenericRecord] = SqlReports.map { report =>
        SqlGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", SqlReportsPath, SqlReportsRecords.size)
      SqlReportsRecords.foreach(SqlReportsWriter.write)
      SqlReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : SqlReports.clear()")
      SqlReports.clear()
    }
    if (JobReports.nonEmpty) {
      val JobReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentJobReportsPath, JobGenericRecord.reportSchema)
      // Convert all JobReports to GenericRecords first
      val JobReportsRecords: Seq[GenericRecord] = JobReports.map { report =>
        JobGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", JobReportsPath, JobReportsRecords.size)
      JobReportsRecords.foreach(JobReportsWriter.write)
      JobReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : JobReports.clear()")
      JobReports.clear()
    }
    if (StageReports.nonEmpty) {
      val StageReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentStageReportsPath, StageGenericRecord.reportSchema)
      // Convert all StageReports to GenericRecords first
      val StageReportsRecords: Seq[GenericRecord] = StageReports.map { report =>
        StageGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", StageReportsPath, StageReportsRecords.size)
      StageReportsRecords.foreach(StageReportsWriter.write)
      StageReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : StageReports.clear()")
      StageReports.clear()
    }
    if (TaskReports.nonEmpty) {
      val TaskReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(currentTaskReportsPath, TaskGenericRecord.reportSchema)
      // Convert all TaskReports to GenericRecords first
      val TaskReportsRecords: Seq[GenericRecord] = TaskReports.map { report =>
        TaskGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      logger.debug("ParquetSink Debug : writing to {} ({} reports).", TaskReportsPath, TaskReportsRecords.size)
      TaskReportsRecords.foreach(TaskReportsWriter.write)
      TaskReportsWriter.close()

      // clear reports
      logger.debug("ParquetSink Debug : TaskReports.clear()")
      TaskReports.clear()
    }
  }

  override def flush(): Unit = {
    logger.debug("ParquetSink Debug : flush")
    write()
  }
}