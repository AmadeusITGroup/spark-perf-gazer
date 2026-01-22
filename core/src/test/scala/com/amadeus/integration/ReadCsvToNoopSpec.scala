package com.amadeus.integration

import com.amadeus.perfgazer.PerfGazer
import com.amadeus.perfgazer.reports._
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark
import org.scalatest.GivenWhenThen

class ReadCsvToNoopSpec extends SimpleSpec with GivenWhenThen {

  describe("The listener when reading a .csv and writing to noop") {
    it("should build reports for a noop write") {
      withSpark(appName = this.getClass.getName) { spark =>
        Given("a Spark session with PerfGazer listeners")
        val sinks = new TestableSink()
        val emptySinks = new TestableSink()
        val df = OptdSupport.readOptd(spark)

        val cfg = defaultTestConfig.withAllEnabled
        val eventsListener = new PerfGazer(cfg, sinks)
        spark.sparkContext.addSparkListener(eventsListener)

        val emptyEventsListener = new PerfGazer(cfg.withAllDisabled, emptySinks)
        spark.sparkContext.addSparkListener(emptyEventsListener)

        When("a CSV is read and written to noop")
        spark.sparkContext.setJobGroup("testgroup", "testjob")
        df.write.format("noop").mode("overwrite").save()

        // Wait for listener asynchronous operations before removing it from sparkContext
        Thread.sleep(3000)
        spark.sparkContext.removeSparkListener(eventsListener)
        spark.sparkContext.removeSparkListener(emptyEventsListener)
        eventsListener.close()
        emptyEventsListener.close()

        Then("it should build some reports")
        sinks.reports.size shouldBe 4

        val sqlReports = sinks.reports.collect { case r: SqlReport => r }
        sqlReports.size should be(1)
        val sqlReport = sqlReports.head

        And("it should build SQL nodes with job name and node name")
        val nodes = sqlReport.nodes
        val sqlId = sqlReport.sqlId
        nodes.size should be(2)
        val nodeTuples = nodes.map(i => (i.sqlId, i.jobName, i.nodeName))
        nodeTuples should contain((sqlId, "testjob", "() OverwriteByExpression"))
        nodeTuples should contain((sqlId, "testjob", "() Scan csv "))

        And("it should build SQL reports with metrics")
        val csvNodes = sqlReport.nodes.filter(_.nodeName contains "Scan csv")
        csvNodes.size should be(1)
        val csvNode = csvNodes.head
        csvNode.metrics.keys should contain("number of files read")

        And("it should build SQL reports with details")
        val sqlDetails = sqlReport.details
        sqlDetails should include regex "== Parsed Logical Plan =="
        sqlDetails should include regex "== Optimized Logical Plan =="
        sqlDetails should include regex "== Physical Plan =="

        And("it should build job reports")
        val jobReports = sinks.reports.collect { case r: JobReport => r }
        jobReports.size should be(1)
        val jobReport = jobReports.head
        jobReport.jobId should be >= 0L
        jobReport.groupId should be("testgroup")
        jobReport.jobName should be("testjob")
        jobReport.sqlId should be(sqlId.toString)
        jobReport.stages.size should be(1)

        And("it should build stage reports (StagePrettyTranslator)")
        val stageReports = sinks.reports.collect { case r: StageReport => r }
        stageReports.size should be(1)
        val stageReport = stageReports.head
        jobReport.stages should contain(stageReport.stageId)
        stageReport.stageId should be >= 0
        stageReport.shuffleReadBytes should be(0)
        stageReport.shuffleWriteBytes should be(0)
        stageReport.attempt should be(0)
        stageReport.readBytes should be > 30L * 1024 * 1024
        stageReport.writeBytes should be(0) // noop
        stageReport.execCpuNs should be > 0L

        And("it should not generate any report if all is disabled")
        emptySinks.reports.size should be(0)
      }
    }
  }
}
