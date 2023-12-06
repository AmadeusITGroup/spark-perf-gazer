package com.amadeus.sparklear.translators

import com.amadeus.sparklear.collects.JobCollect.EndUpdate
import com.amadeus.sparklear.collects.{JobCollect, StageCollect, StageRef}
import com.amadeus.sparklear.prereports.JobPreReport
import com.amadeus.sparklear.reports.StrJobReport
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.scheduler.SparkListenerJobEnd
import org.apache.spark.scheduler.JobSucceeded

class JobTranslatorSpec extends SimpleSpec with ConfigSupport {

  describe(s"${JobPrettyTranslator.getClass.getSimpleName}") {
    it("should generate a simple job report") {
      val c = defaultTestConfig
      val jc = JobCollect(
        name = "job",
        group = "group",
        sqlId = "1",
        initialStages = Seq.empty[StageRef]
      )
      val eu = EndUpdate(
        finalStages = Seq.empty[(StageRef, Option[StageCollect])],
        jobEnd = SparkListenerJobEnd(1, 0, JobSucceeded)
      )
      val p = JobPreReport(jc, eu)
      val r = JobPrettyTranslator.toReports(c, p)
      r should equal(Seq(StrJobReport("JOB ID=1 GROUP='group' NAME='job' SQL_ID=1 STAGES=0 TOTAL_CPU_SEC=0")))

    }
  }

}
