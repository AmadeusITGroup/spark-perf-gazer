package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobGenericRecord, JobReport, Report, SqlGenericRecord, SqlReport, StageGenericRecord, StageReport, TaskReport, TaskGenericRecord}
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

/**
  * Sink of a collection of reports
  */
class ParquetSink(
  // spark: SparkSession,
  destination: String = "src/test/parquet-sink",
  writeBatchSize: Int = 5,
  debug: Boolean = true
) extends Sink {
  private var reportsCount: Int = 0
  private val SqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val JobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val StageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()
  private val TaskReports: ListBuffer[TaskReport] = new ListBuffer[TaskReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  private def getAvroParquetWriter(Path: String, Schema: Schema) : ParquetWriter[GenericRecord] = {
    val OutputPath = new Path(Path)
    val OutputFile: OutputFile = HadoopOutputFile.fromPath(OutputPath, new Configuration())
    AvroParquetWriter
      .builder(OutputFile)
      .withSchema(Schema)
      .withWriteMode(Mode.OVERWRITE)
      .build()
  }

  // Create Parquet writers
  private val SqlReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(s"$destination/sql-reports.parquet", SqlGenericRecord.reportSchema)
  private val JobReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(s"$destination/job-reports.parquet", JobGenericRecord.reportSchema)
  private val StageReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(s"$destination/stage-reports.parquet", StageGenericRecord.reportSchema)
  private val TaskReportsWriter: ParquetWriter[GenericRecord] = getAvroParquetWriter(s"$destination/task-reports.parquet", TaskGenericRecord.reportSchema)

  override def sink(rs: Seq[Report]): Unit = {
    reportsCount += rs.size

    // appends new reports in sink
    rs.map {
      case sql: SqlReport => SqlReports ++= Seq(sql)
      case job: JobReport => JobReports ++= Seq(job)
      case stage: StageReport => StageReports ++= Seq(stage)
      case task: TaskReport => TaskReports ++= Seq(task)
    }

    if ( reportsCount >= writeBatchSize ) {
      println(s"ParquetSink Debug : reached writeBatchSize threshold, writing reports ...")
      write()
      reportsCount = 0
    }
  }

  def write(): Unit = {
    if (SqlReports.nonEmpty) {
      val SqlReportsRecords: Seq[GenericRecord] = SqlReports.map { report =>
        SqlGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      if (debug) {
        println(s"ParquetSink Debug : writing to $destination/sql-reports.parquet (${SqlReportsRecords.size} reports).")
      }
      SqlReportsRecords.foreach(SqlReportsWriter.write)
      //SqlReportsWriter.wait()

      // clear reports
      if (debug) { println("ParquetSink Debug : SqlReports.clear()") }
      SqlReports.clear()
    }
    if (JobReports.nonEmpty) {
      // Convert all JobReports to GenericRecords first
      val JobReportsRecords: Seq[GenericRecord] = JobReports.map { report =>
        JobGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      if (debug) {
        println(s"ParquetSink Debug : writing to $destination/job-reports.parquet (${JobReportsRecords.size} reports).")
      }
      JobReportsRecords.foreach(JobReportsWriter.write)
      //JobReportsWriter.wait()

      // clear reports
      if (debug) { println("ParquetSink Debug : JobReports.clear()") }
      JobReports.clear()
    }
    if (StageReports.nonEmpty) {
      // Convert all StageReports to GenericRecords first
      val StageReportsRecords: Seq[GenericRecord] = StageReports.map { report =>
        StageGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      if (debug) {
        println(s"ParquetSink Debug : writing to $destination/stage-reports.parquet (${StageReportsRecords.size} reports).")
      }
      StageReportsRecords.foreach(StageReportsWriter.write)
      //StageReportsWriter.wait()

      // clear reports
      if (debug) { println("ParquetSink Debug : StageReports.clear()") }
      StageReports.clear()
    }
    if (TaskReports.nonEmpty) {
      // Convert all TaskReports to GenericRecords first
      val TaskReportsRecords: Seq[GenericRecord] = TaskReports.map { report =>
        TaskGenericRecord.fromReportToGenericRecord(report)
      }

      // Write all records in a single loop
      if (debug) {
        println(s"ParquetSink Debug : writing to $destination/task-reports.parquet (${TaskReportsRecords.size} reports).")
      }
      TaskReportsRecords.foreach(TaskReportsWriter.write)
      //TaskReportsWriter.wait()

      // clear reports
      if (debug) { println("ParquetSink Debug : TaskReports.clear()") }
      TaskReports.clear()
    }
  }

  override def flush(): Unit = {
    if (debug) { println(f"ParquetSink Debug : flush") }
    write()

    // Flush and close writers
    TaskReportsWriter.close()
    StageReportsWriter.close()
    JobReportsWriter.close()
    SqlReportsWriter.close()

    if (debug) { println(f"ParquetSink Debug : writers closed.") }
  }
}