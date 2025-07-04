package com.amadeus.sparklear.reports

import com.amadeus.sparklear.events.StageEvent
import com.amadeus.sparklear.entities.StageEntity
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.Fixtures2

class StageReportSpec extends SimpleSpec with ConfigSupport {
  describe(s"The ${StageReport.getClass.getName}") {
    it("should generate reports in a basic scenario") {
      val reportStage = StageEntity(StageEvent(Fixtures2.Stage1.stageInfo))
      val rs = StageReport.fromEntityToReport(reportStage)
      rs shouldEqual (
        StageReport(
          stageId = 1,
          readBytes = 5L*1024*1024,
          writeBytes = 74L*1024*1024,
          shuffleReadBytes = 75L*1024*1024,
          shuffleWriteBytes = 76L*1024*1024,
          execCpuNs = 77L*1000*1000*1000,
          execRunNs = 98L*1000*1000*1000,
          execJvmGcNs = 13L*1000*1000*1000,
          attempt = 8,
          spillBytes = 3L*1024*1024
        )
      )
    }
  }
}
