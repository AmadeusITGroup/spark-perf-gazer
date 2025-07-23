package com.amadeus.sparklear.utils

import com.amadeus.sparklear.JsonSink
import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}

import java.io.File
import java.time.Instant

class JsonSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("json sink") {
    it("should write job reports") {
      withTmpDir { tmpDir =>
        val sparkApplicationId: String = java.util.UUID.randomUUID().toString
        val jsonSink = new JsonSink(
          sparkApplicationId = sparkApplicationId,
          destination = s"$tmpDir",
          writeBatchSize = 1)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, 1000, "1", Seq(1))
        jsonSink.sink(jr)

        val jsonFile = new File(s"$tmpDir/$sparkApplicationId/job-reports.json")
        jsonFile.length() should not equal(0)
      }
    }
  }
}
