package com.amadeus.sparklear.translators

import com.amadeus.sparklear.events.JobEvent.EndUpdate
import com.amadeus.sparklear.events.{JobEvent, StageEvent, StageRef}
import com.amadeus.sparklear.entities.JobEntity
import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.Fixtures2
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd}

class JobTranslatorSpec extends SimpleSpec with ConfigSupport {

  describe(s"${JobReport.getClass.getSimpleName}") {
    it("should generate a simple job report") {
      val c = defaultTestConfig
      val jc = JobEvent(
        name = "job",
        group = "group",
        sqlId = "3",
        initialStages = Seq.empty[StageRef]
      )
      val si = Fixtures2.Stage1.stageInfo
      val sc = StageEvent(si)
      val eu = EndUpdate(
        finalStages = Seq((StageRef(1, 1), Some(sc))),
        jobEnd = SparkListenerJobEnd(7, 0L, JobSucceeded)
      )
      val p = JobEntity(jc, eu)
      val r = JobReport.fromEntityToReport(p)
      r should equal(
        JobReport(
          jobId = 7,
          groupId = "group",
          jobName = "job",
          sqlId = "3",
          stages = 0
        )
      )
    }
  }

}
