package com.amadeus.sparklear.translators

import com.amadeus.sparklear.raw.StageRawEvent
import com.amadeus.sparklear.prereports.StagePreReport
import com.amadeus.sparklear.reports.{SqlPlanNodeReport, StrReport, StrStageReport}
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.Fixtures2

class StageTranslatorSpec extends SimpleSpec with ConfigSupport {
  describe(s"The ${StageTranslator.getClass.getName}") {
    it("should generate reports in a basic scenario") {
      val reportStage = StagePreReport(StageRawEvent(Fixtures2.Stage1.stageInfo))
      val cfg = defaultTestConfig
      val rs = StagePrettyTranslator.toReports(cfg, reportStage)
      rs shouldEqual (
        List(
          StrStageReport(
            "STAGE ID=1 READ_MB=5 WRITE_MB=74 SHUFFLE_READ_MB=75 SHUFFLE_WRITE_MB=76 EXEC_CPU_SECS=77 EXEC_RUN_SECS=98 EXEC_JVM_GC_SECS=13 ATTEMPT=8 SPILL_MB=3"
          )
        )
      )
    }
  }
}
