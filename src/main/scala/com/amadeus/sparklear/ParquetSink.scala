package com.amadeus.sparklear

import com.amadeus.sparklear.reports.{JobReport, Report, SqlReport, StageReport}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{SparkSession, DataFrame}

/**
  * Sink of a collection of reports
  */
class ParquetSink(
  spark: SparkSession,
  destination: String = "src/test/parquet-sink",
  writeBatchSize: Int = 5,
  debug: Boolean = true
) extends Sink {
  // val spark: SparkSession = SparkSession.builder().getOrCreate()
  import spark.implicits._

  private var reportsCount: Int = 0
  private var writeMode: String = "overwrite"
  private val SqlReports: ListBuffer[SqlReport] = new ListBuffer[SqlReport]()
  private val JobReports: ListBuffer[JobReport] = new ListBuffer[JobReport]()
  private val StageReports: ListBuffer[StageReport] = new ListBuffer[StageReport]()

  implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  private def writeReports(): Unit = {
    println(s"ParquetSink Debug : Spark Seesion used : ${spark.sparkContext}")

    if (SqlReports.nonEmpty) {
      if (debug) {
        println(s"ParquetSink Debug : reached writeBatchSize threshold, writing to $destination/sql-reports.parquet (${SqlReports.size} reports).")
      }
      val dfSqlReports: DataFrame = SqlReports.toDF
      dfSqlReports.show()
      try {
        dfSqlReports.write.mode(writeMode).parquet(s"$destination/sql-reports.parquet")
      }
      catch {
        case e: Exception => println(e.getMessage())
      }

      // clear reports
      SqlReports.clear()
    }
    if (JobReports.nonEmpty) {
      if (debug) {
        println(s"ParquetSink Debug : reached writeBatchSize threshold, writing to $destination/job-reports.parquet (${JobReports.size} reports).")
      }
      val dfJobReports: DataFrame = JobReports.toDF
      dfJobReports.show()
      try {
        dfJobReports.write.mode(writeMode).parquet(s"$destination/job-reports.parquet")
      }
      catch {
        case e: Exception => println(e.getMessage())
      }

      // clear reports
      JobReports.clear()
    }
    if (StageReports.nonEmpty) {
      if (debug) {
        println(s"ParquetSink Debug : reached writeBatchSize threshold, writing to $destination/stage-reports.parquet (${StageReports.size} reports).")
      }
      val dfStageReports: DataFrame = StageReports.toDF
      dfStageReports.show()
      try {
        dfStageReports.write.mode(writeMode).parquet(s"$destination/stage-reports.parquet")
      }
      catch {
        case e: Exception => println(e.getMessage())
      }

      // clear reports
      StageReports.clear()
    }
  }

  override def sink(rs: Seq[Report]): Unit = {
    reportsCount += rs.size

    // appends new reports in sink
    rs.map {
      case sql: SqlReport => SqlReports ++= Seq(sql)
      case job: JobReport => JobReports ++= Seq(job)
      case stage: StageReport => StageReports ++= Seq(stage)
    }

    if ( reportsCount >= writeBatchSize ) {
      writeReports()
      reportsCount = 0
      writeMode = "append"
    }
  }

  override def flush(): Unit = {
    if (debug) {
      println(s"ParquetSink Debug : flush sink")
    }
    writeReports()
  }
}