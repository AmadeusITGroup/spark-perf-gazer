package com.amadeus.sparklear.translators

import com.amadeus.sparklear.events.StageEvent
import com.amadeus.sparklear.entities.StageEntity
import com.amadeus.sparklear.reports.StageReport
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.Fixtures2

class StageTranslatorSpec extends SimpleSpec with ConfigSupport {
  describe(s"The ${StageReport.getClass.getName}") {
    it("should generate reports in a basic scenario") {
      val reportStage = StageEntity(StageEvent(Fixtures2.Stage1.stageInfo))
      val cfg = defaultTestConfig
      val rs = StageReport.fromEntityToReport(reportStage)
      rs shouldEqual (
        StageReport(
          stageId = 1,
          readMb = 5,
          writeMb = 74,
          shuffleReadMb = 75,
          shuffleWriteMb = 76,
          execCpuSecs = 77,
          execRunSecs = 98,
          execJvmGcSecs = 13,
          attempt = 8,
          spillMb = 3L
        )
      )
    }
  }
}
