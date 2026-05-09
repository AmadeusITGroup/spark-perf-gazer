package com.amadeus.integration

import com.amadeus.perfgazer.{AnalysisQueries, JsonSink, PerfGazer}
import com.amadeus.perfgazer.reports._
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.lit
import org.scalatest.GivenWhenThen

class ReadCsvToNoopSpec extends SimpleSpec with GivenWhenThen {

  describe("The listener when reading a .csv and writing to noop") {
    it("should build reports for a noop write") {
      withSpark(appName = this.getClass.getName) { spark =>
        withTmpDir { tmpDir =>
          Given("a Spark session with PerfGazer listeners")
          val destination = s"$tmpDir/perfgazer-output"
          val sparkConf = new SparkConf(false)
            .set(JsonSink.DestinationKey, destination)
          val jsonSink = new JsonSink(sparkConf)

          val sinks = new TestableSink()
          val emptySinks = new TestableSink()
          val df = OptdSupport.readOptd(spark)

          val cfg = defaultTestConfig.withAllEnabled
          val eventsListener = new PerfGazer(cfg, jsonSink)
          spark.sparkContext.addSparkListener(eventsListener)

          val sinkEventsListener = new PerfGazer(cfg, sinks)
          spark.sparkContext.addSparkListener(sinkEventsListener)

          val emptyEventsListener = new PerfGazer(cfg.withAllDisabled, emptySinks)
          spark.sparkContext.addSparkListener(emptyEventsListener)

          When("a small job writes the CSV once to noop")
          spark.sparkContext.setJobGroup("testgroup", "testjobsmall")
          df.write.format("noop").mode("overwrite").save()

          And("a large job writes the CSV 10 times to noop")
          spark.sparkContext.setJobGroup("testgroup", "testjoblarge")
          for (i <- 0 until 10) {
            df.withColumn("instance", lit(i)).write.format("noop").mode("overwrite").save()
          }

          // Wait for listener asynchronous operations before removing it from sparkContext
          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          spark.sparkContext.removeSparkListener(sinkEventsListener)
          spark.sparkContext.removeSparkListener(emptyEventsListener)
          eventsListener.close()
          sinkEventsListener.close()
          emptyEventsListener.close()

          Then("the listener should build some reports")
          sinks.reports should not be empty

          And("it should build SQL nodes with job name and node name")
          val sqlReports = sinks.reports.collect { case r: SqlReport => r }
          val sqlReport = sqlReports.find(_.nodes.exists(_.jobName == "testjobsmall")).get
          val nodes = sqlReport.nodes
          nodes.size should be(2)
          val sqlId = sqlReport.sqlId
          nodes.map(i => (i.sqlId, i.jobName, i.nodeName)).head should be(
            sqlId, "testjobsmall", "() OverwriteByExpression"
          )
          nodes.map(i => (i.sqlId, i.jobName, i.nodeName)).last should be(
            sqlId, "testjobsmall", "() Scan csv "
          )

          And("it should build SQL reports with metrics")
          val csvNodes = sqlReport.nodes.filter(_.nodeName contains "Scan csv")
          csvNodes.size should be(1)
          csvNodes.head.metrics.keys should contain("number of files read")

          And("it should build SQL reports with details")
          val sqlDetails = sqlReport.details
          sqlDetails should include regex "== Parsed Logical Plan =="
          sqlDetails should include regex "== Optimized Logical Plan =="
          sqlDetails should include regex "== Physical Plan =="

          And("it should build job reports")
          val jobReports = sinks.reports.collect { case r: JobReport => r }
          val jobReport = jobReports.find(_.jobName == "testjobsmall").get
          jobReport.groupId should be("testgroup")
          jobReport.jobName should be("testjobsmall")
          jobReport.sqlId should be(sqlReport.sqlId.toString)
          jobReport.stages should not be empty

          And("it should build stage reports")
          val stageReports = sinks.reports.collect { case r: StageReport => r }
          val stageReport = stageReports.find(s => jobReport.stages.contains(s.stageId)).get
          stageReport.shuffleReadBytes should be(0)
          stageReport.shuffleWriteBytes should be(0)
          stageReport.attempt should be(0)
          stageReport.readBytes should be > 30L * 1024 * 1024
          stageReport.writeBytes should be(0) // noop
          stageReport.execCpuNs should be > 0L

          And("it should not generate any report if all is disabled")
          emptySinks.reports.size should be(0)

          And("it should report that the large job has more CPU usage than the small on")
          val snippets = eventsListener.getSnippets
          snippets.foreach(spark.sql)

          val cpuResults = spark.sql(AnalysisQueries.JobsByCpuUsage).collect()
          val cpuByJob = cpuResults.map(r => (r.getAs[String]("jobName"), r.getAs[Double]("cpuTimeSec")))
          val largeCpu = cpuByJob.filter(_._1 == "testjoblarge").map(_._2).sum
          val smallCpu = cpuByJob.filter(_._1 == "testjobsmall").map(_._2).sum
          largeCpu should be > smallCpu

          And("the large job should have more I/O than the small job")
          val ioResults = spark.sql(AnalysisQueries.JobsByIoVolumes).collect()
          val ioByJob = ioResults.map(r => (r.getAs[String]("jobName"), r.getAs[Double]("inputMb")))
          val largeIo = ioByJob.filter(_._1 == "testjoblarge").map(_._2).sum
          val smallIo = ioByJob.filter(_._1 == "testjobsmall").map(_._2).sum
          largeIo should be > smallIo

          And("skew detection should return results with expected columns")
          val skewResults = spark.sql(AnalysisQueries.SkewDetection).collect()
          // In local[1] mode skew is unlikely, but the query must execute and return the right schema
          val skewColumns = spark.sql(AnalysisQueries.SkewDetection).columns.toSet
          skewColumns should contain allOf("jobId", "jobName", "stageId", "taskCount",
            "maxDurationSec", "medianDurationSec", "p75DurationSec", "stddevDurationSec", "skewFactor")

        }
      }
    }
  }
}
