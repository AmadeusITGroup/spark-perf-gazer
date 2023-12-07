package com.amadeus.sparklear.translators

import com.amadeus.sparklear.collects.JobCollect.EndUpdate
import com.amadeus.sparklear.collects.{JobCollect, StageCollect, StageRef}
import com.amadeus.sparklear.prereports.JobPreReport
import com.amadeus.sparklear.reports.StrJobReport
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.Fixtures2
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd}

class JobTranslatorSpec extends SimpleSpec with ConfigSupport {

  describe(s"${JobPrettyTranslator.getClass.getSimpleName}") {
    it("should generate a simple job report") {
      val c = defaultTestConfig
      val jc = JobCollect(
        name = "job",
        group = "group",
        sqlId = "3",
        initialStages = Seq.empty[StageRef]
      )
      val si = Fixtures2.Stage1.stageInfo
      val sc = StageCollect(si)
      val eu = EndUpdate(
        finalStages = Seq((StageRef(1, 1), Some(sc))),
        jobEnd = SparkListenerJobEnd(7, 0L, JobSucceeded)
      )
      val p = JobPreReport(jc, eu)
      val r = JobPrettyTranslator.toReports(c, p)
      r should equal(Seq(StrJobReport("JOB ID=7 GROUP='group' NAME='job' SQL_ID=3 SPILL_MB=3 STAGES=0 TOTAL_CPU_SEC=77")))
    }
  }

}
