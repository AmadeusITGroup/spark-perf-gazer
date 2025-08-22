package com.amadeus.sparklear.utils

import com.amadeus.sparklear.LogSink
import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}


import java.time.Instant

class LogSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("log sink") {
    it("should write log") {
      val logSink = new LogSink()

      val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, 1000, "1", Seq(1))
      logSink.sink(jr)
      logSink.write()
      logSink.flush()
    }
  }
}
