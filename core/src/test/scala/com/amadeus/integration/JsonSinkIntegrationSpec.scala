package com.amadeus.integration

import com.amadeus.perfgazer.{JsonSink, PerfGazer}
import com.amadeus.perfgazer.PathBuilder.PathOps
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import org.apache.spark.SparkConf

class JsonSinkIntegrationSpec
  extends SimpleSpec {

  describe("The listener when reading a .csv and writing to noop") {
    it("should write and reconcile json reports") {
      withSpark(appName = this.getClass.getName) { spark =>
        withTmpDir { tmpDir =>
          // Set thresholds for coverage - only flush at the end
          val destination = s"$tmpDir".withDefaultPartitions.resolveProperties(spark.sparkContext.getConf)
          val writeBatchSize = 200
          val fileSizeLimit = 200L*1024*1024
          val sparkConf = new SparkConf(false)
            .set(JsonSink.DestinationKey, destination)
            .set(JsonSink.WriteBatchSizeKey, writeBatchSize.toString)
            .set(JsonSink.FileSizeLimitKey, fileSizeLimit.toString)
          val jsonSink = new JsonSink(sparkConf)

          import org.apache.spark.sql.functions._

          val df = OptdSupport.readOptd(spark)

          // regular setup
          val cfg = defaultTestConfig.withAllEnabled
          val eventsListener = new PerfGazer(cfg, jsonSink)
          spark.sparkContext.addSparkListener(eventsListener)

          spark.sparkContext.setJobGroup("testgroup", "testjob")
          df.write.format("noop").mode("overwrite").save()

          // Wait for listener asynchronous operations before removing it from sparkContext
          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          eventsListener.close()

          val dfSqlReports = spark.read.json(s"$destination/sql-reports-*.json")
          val dfSqlReportsCnt = dfSqlReports.count()
          dfSqlReports.schema.names.toSet should contain("sqlId")
          dfSqlReportsCnt shouldBe 1

          val dfJobReports = spark.read.json(s"$destination/job-reports-*.json")
          val dfJobReportsCnt = dfJobReports.count()
          dfJobReports.schema.names.toSet should contain("jobId")
          dfJobReportsCnt shouldBe 1

          val dfStageReports = spark.read.json(s"$destination/stage-reports-*.json")
          val dfStageReportsCnt = dfStageReports.count()
          dfStageReports.schema.names.toSet should contain("stageId")
          dfStageReportsCnt shouldBe 1

          val dfTaskReports = spark.read.json(s"$destination/task-reports-*.json")
          val dfTaskReportsCnt = dfTaskReports.count()
          dfTaskReports.schema.names.toSet should contain("taskId")
          dfTaskReportsCnt shouldBe 1

          // Reconcile reports from json files
          val dfTasks = dfJobReports
            .withColumn("stageId", explode(col("stages")))
            .drop("stages")
            .join(dfStageReports, Seq("stageId"))
            .join(dfTaskReports, Seq("stageId"))
          val dfTasksCnt = dfTasks.count()

          dfTasksCnt should equal(dfTaskReportsCnt)
        }
      }
    }
  }
}
