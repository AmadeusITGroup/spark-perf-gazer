package com.amadeus.sparklear

import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}

import java.io.File
import java.time.Instant

class JsonSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("json sink") {
    it("should write job reports with writeBatchSize = 1") {
      withTmpDir { tmpDir =>
        val jsonSink = new JsonSink(
          destination = s"$tmpDir",
          writeBatchSize = 1)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonFile = new File(s"$tmpDir/job-reports.json")
        jsonFile.length() should equal(0)

        jsonSink.write(jr)
        jsonFile.length() should not equal 0

        jsonSink.close()
        jsonFile.length() should not equal 0
      }
    }
    it("should write job reports with writeBatchSize = 5") {
      withTmpDir { tmpDir =>
        val jsonSink = new JsonSink(
          destination = s"$tmpDir",
          writeBatchSize = 5)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonFile = new File(s"$tmpDir/job-reports.json")

        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should not equal 0
      }
    }
    it("should write job reports when writeBatchSize not reached and sink is flushed") {
      withTmpDir { tmpDir =>
        val jsonSink = new JsonSink(
          destination = s"$tmpDir",
          writeBatchSize = 5)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonFile = new File(s"$tmpDir/job-reports.json")

        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)
        jsonSink.write(jr)
        jsonFile.length() should equal(0)

        // flush with 4 reports in sink
        jsonSink.close()
        jsonFile.length() should not equal 0
      }
    }
  }
}
