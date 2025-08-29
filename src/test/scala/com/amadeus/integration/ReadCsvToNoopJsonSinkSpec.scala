package com.amadeus.integration

import com.amadeus.sparklear.SparklEar
import com.amadeus.testfwk._

class ReadCsvToNoopJsonSinkSpec
  extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with TempDirSupport
    with SinkSupport {

  describe("The listener when reading a .csv and writing to noop") {
    withSpark() { spark =>
      withTmpDir { tmpDir =>

        val writeBatchSize = 5
        withJsonSink(spark.sparkContext.applicationId, s"$tmpDir", writeBatchSize) { jsonSink =>
          import org.apache.spark.sql.functions._

          val df = readOptd(spark)

          // regular setup
          val cfg = defaultTestConfig.withAllEnabled.withSink(jsonSink)
          val eventsListener = new SparklEar(cfg)
          spark.sparkContext.addSparkListener(eventsListener)

          spark.sparkContext.setJobGroup("testgroup", "testjob")
          df.write.format("noop").mode("overwrite").save()

          // Wait for listener asynchronous operations before removing it from sparkContext
          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          jsonSink.close()

          val dfSqlReports = spark.read.json(jsonSink.sqlReportsPath)
          val dfSqlReportsCnt = dfSqlReports.count()
          it("should save SQL reports in json file") {
            dfSqlReportsCnt shouldBe 1
          }
          dfSqlReports.show()

          val dfJobReports = spark.read.json(jsonSink.jobReportsPath)
          val dfJobReportsCnt = dfJobReports.count()
          it("should save Job reports in json file") {
            dfJobReportsCnt shouldBe 1
          }

          val dfStageReports = spark.read.json(jsonSink.stageReportsPath)
          val dfStageReportsCnt = dfStageReports.count()
          it("should save Stage reports in json file") {
            dfStageReportsCnt shouldBe 1
          }

          val dfTaskReports = spark.read.json(jsonSink.taskReportsPath)
          val dfTaskReportsCnt = dfTaskReports.count()
          it("should save Task reports in json file") {
            dfTaskReportsCnt shouldBe 1
          }

          // Reconcile reports from json files
          val dfTasks = dfJobReports
            .withColumn("stageId", explode(col("stages")))
            .drop("stages")
            .join(dfStageReports, Seq("stageId"))
            .drop(dfStageReports("stageId"))
            .join(dfTaskReports, Seq("stageId"))
            .drop(dfTaskReports("stageId"))
          dfTasks.show()
        }
      }
    }
  }
}
