package com.amadeus.sparklear.utils

import com.amadeus.sparklear.ParquetSink
import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}

import java.io.File
import java.io.FilenameFilter
import java.time.Instant

class ParquetSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("parquet sink") {
    it("should write job reports with writeBatchSize = 1") {
      withTmpDir { tmpDir =>
        val sparkApplicationId: String = java.util.UUID.randomUUID().toString
        val parquetSink = new ParquetSink(
          sparkApplicationId = sparkApplicationId,
          destination = s"$tmpDir",
          writeBatchSize = 1)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val parquetLocation = new File(s"$tmpDir/$sparkApplicationId/job-reports.parquet")

        parquetSink.write(jr)
        parquetLocation.listFiles().count(file => file.isFile && file.getName.endsWith(".parquet")) should equal(1)

        parquetSink.close()
        parquetLocation.listFiles().count(file => file.isFile && file.getName.endsWith(".parquet")) should equal(1)
      }
    }
    it("should write job reports with writeBatchSize = 5") {
      withTmpDir { tmpDir =>
        val sparkApplicationId: String = java.util.UUID.randomUUID().toString
        val parquetSink = new ParquetSink(
          sparkApplicationId = sparkApplicationId,
          destination = s"$tmpDir",
          writeBatchSize = 5)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val parquetLocation = new File(s"$tmpDir/$sparkApplicationId/job-reports.parquet")

        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.listFiles().count(file => file.isFile && file.getName.endsWith(".parquet")) should equal(1)
      }
    }
    it("should write job reports when writeBatchSize not reached and sink is flushed") {
      withTmpDir { tmpDir =>
        val sparkApplicationId: String = java.util.UUID.randomUUID().toString
        val parquetSink = new ParquetSink(
          sparkApplicationId = sparkApplicationId,
          destination = s"$tmpDir",
          writeBatchSize = 5)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val parquetLocation = new File(s"$tmpDir/$sparkApplicationId/job-reports.parquet")

        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)
        parquetSink.write(jr)
        parquetLocation.exists() should equal(false)

        // flush with 4 reports in sink
        parquetSink.close()
        parquetLocation.listFiles().count(file => file.isFile && file.getName.endsWith(".parquet")) should equal(1)
      }
    }
  }
}
