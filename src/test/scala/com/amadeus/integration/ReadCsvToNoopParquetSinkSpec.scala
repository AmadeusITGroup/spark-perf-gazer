package com.amadeus.integration

import com.amadeus.sparklear.SparklEar
import com.amadeus.testfwk._

class ReadCsvToNoopParquetSinkSpec
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
        withParquetSink(s"$tmpDir", writeBatchSize) { parquetSink =>
          import org.apache.spark.sql.functions._

          val df = readOptd(spark)

          // regular setup
          val cfg = defaultTestConfig.withAllEnabled.withSink(parquetSink)
          val eventsListener = new SparklEar(cfg)
          spark.sparkContext.addSparkListener(eventsListener)

          spark.sparkContext.setJobGroup("testgroup", "testjob")
          df.write.format("noop").mode("overwrite").save()

          // Wait for listener asynchronous operations before removing it from sparkContext
          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          parquetSink.close()

          val dfSqlReports = spark.read.parquet(parquetSink.sqlReportsDirPath)
          val dfSqlReportsCnt = dfSqlReports.count()
          it("should save SQL reports in parquet file") {
            dfSqlReportsCnt shouldBe 1
          }
          dfSqlReports.show()

          val dfJobReports = spark.read.parquet(parquetSink.jobReportsDirPath)
          val dfJobReportsCnt = dfJobReports.count()
          it("should save Job reports in parquet file") {
            dfJobReportsCnt shouldBe 1
          }

          val dfStageReports = spark.read.parquet(parquetSink.stageReportsDirPath)
          val dfStageReportsCnt = dfStageReports.count()
          it("should save Stage reports in parquet file") {
            dfStageReportsCnt shouldBe 1
          }

          val dfTaskReports = spark.read.parquet(parquetSink.taskReportsDirPath)
          val dfTaskReportsCnt = dfTaskReports.count()
          it("should save Task reports in parquet file") {
            dfTaskReportsCnt shouldBe 1
          }

          // Reconcile reports from parquet files
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
