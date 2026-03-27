package com.amadeus.integration

import com.amadeus.perfgazer.PerfGazer
import com.amadeus.perfgazer.reports._
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark

class ReadCsvToNoopSpec extends SimpleSpec {

  describe("The listener when reading a .csv and writing to noop") {
    it("should build reports for a noop write") {
      withSpark(appName = this.getClass.getName) { spark =>
        val sinks = new TestableSink()
        val emptySinks = new TestableSink()
        val df = OptdSupport.readOptd(spark)

        // regular setup
        val cfg = defaultTestConfig.withAllEnabled
        val eventsListener = new PerfGazer(cfg, sinks)
        spark.sparkContext.addSparkListener(eventsListener)

        // setup to check if disabling all entities yields no reports in sinks
        val emptyEventsListener = new PerfGazer(cfg.withAllDisabled, emptySinks)
        spark.sparkContext.addSparkListener(emptyEventsListener)

        spark.sparkContext.setJobGroup("testgroup", "testjob")
        df.write.format("noop").mode("overwrite").save()

        // Wait for listener asynchronous operations before removing it from sparkContext
        Thread.sleep(3000)
        spark.sparkContext.removeSparkListener(eventsListener)
        spark.sparkContext.removeSparkListener(emptyEventsListener)

        // Close the listeners
        eventsListener.close()
        emptyEventsListener.close()

        withClue("should build some reports: ") {
          sinks.reports.size shouldBe 3
        }

        val sqlReports = sinks.reports.collect { case r: SqlReport => r }
        val sqlReport = sqlReports.head

        withClue("should build SQL nodes with job name and node name: ") {
          sqlReports.size should be(1)
          val nodes = sqlReport.nodes
          nodes.size should be(2)
          nodes.map(i => (i.sqlId, i.jobName, i.nodeName)).head should be(1, "testjob", "() OverwriteByExpression")
          nodes.map(i => (i.sqlId, i.jobName, i.nodeName)).last should be(1, "testjob", "() Scan csv ")
        }

        withClue("should build SQL reports with metrics: ") {
          val csvNodes = sqlReport.nodes.filter(_.nodeName contains "Scan csv")
          csvNodes.size should be(1)
          val csvNode = csvNodes.head
          csvNode.metrics.keys should contain("number of files read")
        }

        withClue("should build SQL reports with details: ") {
          val sqlDetails = sqlReport.details
          sqlDetails should include regex "== Parsed Logical Plan =="
          sqlDetails should include regex "== Optimized Logical Plan =="
          sqlDetails should include regex "== Physical Plan =="
        }

        withClue("should build job reports: ") {
          val jobReports = sinks.reports.collect { case r: JobReport => r }
          jobReports.size should be(1)
          val jobReport = jobReports.head
          jobReport.jobId should be(1L)
          jobReport.groupId should be("testgroup")
          jobReport.jobName should be("testjob")
          jobReport.sqlId should be("1")
          jobReport.stages should be(Seq(1))
        }

        withClue("should build stage reports (StagePrettyTranslator): ") {
          val stageReports = sinks.reports.collect { case r: StageReport => r }
          stageReports.size should be(1)
          val stageReport = stageReports.head
          stageReport.stageId should be(1)
          stageReport.shuffleReadBytes should be(0)
          stageReport.shuffleWriteBytes should be(0)
          stageReport.attempt should be(0)
          stageReport.readBytes should be > 30L*1024*1024
          stageReport.writeBytes should be(0) // noop
          stageReport.execCpuNs should be > 0L
        }

        withClue("should not generate any report if all is disabled: ") {
          emptySinks.reports.size should be(0)
        }
      }
    }
  }
}
