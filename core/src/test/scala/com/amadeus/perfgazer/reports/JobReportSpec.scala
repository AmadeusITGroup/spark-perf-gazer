package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.JobEvent.EndUpdate
import com.amadeus.perfgazer.events.{JobEvent, StageEvent, StageRef}
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.Fixtures2
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd}

class JobReportSpec extends SimpleSpec with ConfigSupport {

  describe(s"${JobReport.getClass.getSimpleName}") {
    it("should generate a simple job report") {
      val jc = JobEvent(
        name = "job",
        group = "group",
        sqlId = "3",
        initialStages = Seq(StageRef(id = 0, nroTasks = 0)),
        id = 7,
        startTime = 0L,
      )
      val eu = EndUpdate(
        jobEnd = SparkListenerJobEnd(7, 0L, JobSucceeded)
      )
      val r = JobReport(jc, eu)
      r should equal(
        JobReport(
          jobId = 7,
          groupId = "group",
          jobName = "job",
          sqlId = "3",
          stages = Seq(0),
          jobStartTime = 0L,
          jobEndTime = 0L
        )
      )
    }
  }
}
