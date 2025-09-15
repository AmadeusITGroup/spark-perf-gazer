package com.amadeus.sparklear

import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}

import java.time.Instant

class LogSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("log sink") {
    it("should write log") {
      val logSink = new LogSink()

      val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
      logSink.write(jr)
      logSink.close()

    }
  }
}
