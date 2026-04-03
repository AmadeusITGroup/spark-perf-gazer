package com.amadeus.integration

import com.amadeus.perfgazer.PerfGazer
import com.amadeus.perfgazer.reports._
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.perfgazer.JsonSink
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import org.scalatest.GivenWhenThen

class BasicIntegrationTest extends SimpleSpec with GivenWhenThen {

  describe("The listener when reading a .csv and writing to noop") {
    it("should build reports for a noop write") {
      withSpark(appName = this.getClass.getName) { spark =>
        withTmpDir { tmpDir => 
          Given("a Spark session with PerfGazer listeners")
          val testableSink = new TestableSink()
          val testableSinkEmpty = new TestableSink()
          val jsonSink = new JsonSink(
            JsonSink.Config(destination = tmpDir.toString), spark.sparkContext.getConf)
          val df = OptdSupport.readOptd(spark)

          val testablePerfGazer = new PerfGazer(defaultTestConfig.withAllEnabled, testableSink)
          spark.sparkContext.addSparkListener(testablePerfGazer)

          val emptyPerfGazer = new PerfGazer(defaultTestConfig.withAllDisabled, testableSinkEmpty)
          spark.sparkContext.addSparkListener(emptyPerfGazer)

          val jsonPerfGazer = new PerfGazer(defaultTestConfig.withAllEnabled, jsonSink)
          spark.sparkContext.addSparkListener(jsonPerfGazer)

          When("a CSV is read and written to noop")
          spark.sparkContext.setJobGroup("testgroup", "testjob")
          df.write.format("noop").mode("overwrite").save()

          // Wait for listener asynchronous operations before removing it from sparkContext
          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(testablePerfGazer)
          testablePerfGazer.close()
          spark.sparkContext.removeSparkListener(emptyPerfGazer)
          emptyPerfGazer.close()
          spark.sparkContext.removeSparkListener(jsonPerfGazer)
          jsonPerfGazer.close()


          Then("it should build some reports")
          testableSink.reports.size shouldBe 4

          And("it should build SQL nodes with job name and node name")
          val sqlReports = testableSink.reports.collect { case r: SqlReport => r }
          sqlReports.size should be(1)
          val sqlReport = sqlReports.head
          val nodes = sqlReport.nodes
          nodes.size should be(2)
          nodes.map(i => (i.sqlId, i.jobName, i.nodeName)).head should be(1, "testjob", "() OverwriteByExpression")
          nodes.map(i => (i.sqlId, i.jobName, i.nodeName)).last should be(1, "testjob", "() Scan csv ")

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
          val jobReports = testableSink.reports.collect { case r: JobReport => r }
          jobReports.size should be(1)
          val jobReport = jobReports.head
          jobReport.jobId should be(1L)
          jobReport.groupId should be("testgroup")
          jobReport.jobName should be("testjob")
          jobReport.sqlId should be("1")
          jobReport.stages should be(Seq(1))

          And("it should build stage reports")
          val stageReports = testableSink.reports.collect { case r: StageReport => r }
          stageReports.size should be(1)
          val stageReport = stageReports.head
          stageReport.stageId should be(1)
          stageReport.shuffleReadBytes should be(0)
          stageReport.shuffleWriteBytes should be(0)
          stageReport.attempt should be(0)
          stageReport.readBytes should be > 30L*1024*1024
          stageReport.writeBytes should be(0) // noop
          stageReport.execCpuNs should be > 0L

          And("it should not generate any report if all is disabled")
          testableSinkEmpty.reports.size should be(0)

          And("views should not exist before creation")
          Seq("sql", "job", "stage", "task").foreach { viewName =>
            spark.catalog.tableExists(viewName) should be(false)
          }

          And("it should create views correctly using jsonsink")
          jsonPerfGazer.getSnippets.foreach(s => spark.sql(s))

          And("views should contain data")
          spark.sql("select * from sql").count() should be >= 1L
          spark.sql("select * from job").count() should be >= 1L
          spark.sql("select * from stage").count() should be >= 1L
          spark.sql("select * from task").count() should be >= 1L

          And("views should be joinable")
          val dfTasksCnt = spark.sql("""
            SELECT t.*
            FROM (SELECT explode(stages) AS stageId FROM job) j
            JOIN stage USING (stageId)
            JOIN task t USING (stageId)
          """).count()
          dfTasksCnt should equal(spark.sql("select * from task").count())

        }
      }
    }
  }
}
