package com.amadeus.sparklear.utils

import com.amadeus.sparklear.JsonSink
import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}

import java.time.Instant

class JsonSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("json sink") {
    it("should write job reports") {
      withTmpDir { tmpDir =>
        import java.io.File
        val folder = new File(s"$tmpDir/json-sink")
        if (!folder.exists()) {
          folder.mkdirs()
        }

        val sparkApplicationId: String = java.util.UUID.randomUUID().toString
        val jsonSink = new JsonSink(
          sparkApplicationId = sparkApplicationId,
          destination = s"$tmpDir/json-sink",
          writeBatchSize = 1)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, 1000, "1", Seq(1))
        jsonSink.sink(jr)

        val jsonFile = new File(s"$tmpDir/json-sink/$sparkApplicationId/job-reports.json")
        jsonFile.length() should not equal(0)
      }
    }
  }
}
