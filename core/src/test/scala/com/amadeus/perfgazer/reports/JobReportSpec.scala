package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.JobEvent.EndUpdate
import com.amadeus.perfgazer.events.{JobEvent, StageRef}
import com.amadeus.testfwk.SimpleSpec
import org.apache.spark.Fixtures2
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd, SparkListenerJobStart}
import org.apache.spark.storage.RDDInfo

import java.util.Properties

class JobReportSpec extends SimpleSpec {

  describe(s"${JobReport.getClass.getSimpleName}") {
    it("should generate a simple job report") {
      val jp = new Properties()
      jp.setProperty("spark.job.description", "job")
      jp.setProperty("spark.jobGroup.id", "group")
      jp.setProperty("spark.sql.execution.id", "3")
      jp.setProperty("properties.key1", "value1")
      jp.setProperty("properties.key2", "value2")

      val js = SparkListenerJobStart(
        jobId = 7,
        time = 0L,
        stageInfos = Seq(new org.apache.spark.scheduler.StageInfo(
          stageId = 0,
          name = "stage0",
          numTasks = 1,
          rddInfos = Seq.empty[RDDInfo],
          parentIds = Seq.empty[Int],
          details = "stage details",
          resourceProfileId = 1,
          attemptId = 1,
          taskMetrics = Fixtures2.Stage1.taskMetrics
        )),
        properties = jp
      )
      val je = JobEvent.from(js, "properties.key1")
      val eu = EndUpdate(
        jobEnd = SparkListenerJobEnd(7, 0L, JobSucceeded)
      )
      val r = JobReport.apply(je, eu)
      r should equal(
        JobReport(
          jobId = 7,
          groupId = "group",
          jobName = "job",
          sqlId = "3",
          stages = Seq(0),
          jobStartTime = 0L,
          jobEndTime = 0L,
          jobProperties = Map("properties.key1" -> "value1")
        )
      )
    }
  }
}
