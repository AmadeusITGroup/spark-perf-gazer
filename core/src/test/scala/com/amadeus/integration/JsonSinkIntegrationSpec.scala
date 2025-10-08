package com.amadeus.integration

import com.amadeus.sparklear.{JsonSinkConfig, SparklEar}
import com.amadeus.testfwk._

class JsonSinkIntegrationSpec
  extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with TempDirSupport
    with SinkSupport {

  describe("The listener when reading a .csv and writing to noop") {
    withSpark(appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        // Set thresholds for coverage - only flush at the end
        val writeBatchSize = 200
        val fileSizeLimit = 200L*1024*1024
        withJsonSink(s"$tmpDir", writeBatchSize, fileSizeLimit) { jsonSink =>
          import org.apache.spark.sql.functions._

          val df = readOptd(spark)

          // regular setup
          val cfg = defaultTestConfig.withAllEnabled
          val eventsListener = new SparklEar(cfg, jsonSink)
          spark.sparkContext.addSparkListener(eventsListener)

          spark.sparkContext.setJobGroup("testgroup", "testjob")
          df.write.format("noop").mode("overwrite").save()

          // Wait for listener asynchronous operations before removing it from sparkContext
          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          jsonSink.close()

          val dfSqlReports = spark.read.json(s"$tmpDir/sql-reports-*.json")
          val dfSqlReportsCnt = dfSqlReports.count()
          it("should save SQL reports in json file") {
            dfSqlReports.schema.names.toSet should contain("sqlId")
            dfSqlReportsCnt shouldBe 1
          }
          dfSqlReports.show()

          val dfJobReports = spark.read.json(s"$tmpDir/job-reports-*.json")
          val dfJobReportsCnt = dfJobReports.count()
          it("should save Job reports in json file") {
            dfJobReports.schema.names.toSet should contain("jobId")
            dfJobReportsCnt shouldBe 1
          }

          val dfStageReports = spark.read.json(s"$tmpDir/stage-reports-*.json")
          val dfStageReportsCnt = dfStageReports.count()
          it("should save Stage reports in json file") {
            dfStageReports.schema.names.toSet should contain("stageId")
            dfStageReportsCnt shouldBe 1
          }

          val dfTaskReports = spark.read.json(s"$tmpDir/task-reports-*.json")
          val dfTaskReportsCnt = dfTaskReports.count()
          it("should save Task reports in json file") {
            dfTaskReports.schema.names.toSet should contain("taskId")
            dfTaskReportsCnt shouldBe 1
          }

          // Reconcile reports from json files
          val dfTasks = dfJobReports
            .withColumn("stageId", explode(col("stages")))
            .drop("stages")
            .join(dfStageReports, Seq("stageId"))
            .join(dfTaskReports, Seq("stageId"))
          dfTasks.show()
          val dfTasksCnt = dfTasks.count()

          it("should reconcile reports") {
            dfTasksCnt should equal(dfTaskReportsCnt)
          }

          // Close the listener
          eventsListener.close()
        }
      }
    }
  }
}
