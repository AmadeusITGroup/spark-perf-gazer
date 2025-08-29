package com.amadeus.sparklear

import com.amadeus.sparklear.reports._
import com.amadeus.testfwk._

// Define your case class
case class MakeModel(make: String, model: String)
case class CarRegistration(registration: String, make: String, model: String, engine_size: BigDecimal)
case class CarPrice(make: String, model: String, engine_size: BigDecimal, sale_price: Double)

class SampleSkewDetectionSpec
    extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with SinkSupport {

  describe("The listener for skew detection") {
    withSpark() { spark =>
      withParquetSink(
        spark.sparkContext.applicationId, "src/test/parquet-sink", 5) { parquetSink =>
        import org.apache.spark.sql._
        import org.apache.spark.sql.functions._
        import scala.util.Random
        import spark.implicits._

        // regular setup
        val cfg = defaultTestConfig.withAllEnabled.withSink(parquetSink)
        val eventsListener = new SparklEar(cfg)
        spark.sparkContext.addSparkListener(eventsListener)

        spark.sparkContext.setJobDescription("Prepare skewed data frames")

        val makeModelSet: Seq[MakeModel] = Seq(
            MakeModel("FORD", "FIESTA")
          , MakeModel("NISSAN", "QASHQAI")
          , MakeModel("HYUNDAI", "I20")
          , MakeModel("SUZUKI", "SWIFT")
          , MakeModel("MERCEDED_BENZ", "E CLASS")
          , MakeModel("VAUXHALL", "CORSA")
          , MakeModel("FIAT", "500")
          , MakeModel("SKODA", "OCTAVIA")
          , MakeModel("KIA", "RIO")
        )

        def randomMakeModel(): MakeModel = {
          // returns FORD/FIESTA when Random.nextBoolean() is true (half of the time) and picks a random MakeModel otherwise
          val makeModelIndex = if (Random.nextBoolean()) 0 else Random.nextInt(makeModelSet.size)
          makeModelSet(makeModelIndex)
        }
        def randomEngineSize() = BigDecimal(s"1.${Random.nextInt(9)}")
        def randomRegistration(): String = s"${Random.alphanumeric.take(7).mkString("")}"
        def randomPrice() = 500 + Random.nextInt(5000)

        // cars with registration for which we need to calculate the average price based on similarity
        def randomCarRegistration(): CarRegistration = {
          val makeModel = randomMakeModel()
          CarRegistration(randomRegistration(), makeModel.make, makeModel.model, randomEngineSize())
        }

        // cars with prices
        def randomCarPrice(): CarPrice = {
          val makeModel = randomMakeModel()
          CarPrice(makeModel.make, makeModel.model, randomEngineSize(), randomPrice())
        }

        val small_df_registrations = Seq.fill(10000)(randomCarRegistration()).toDS()
        val large_df_prices = Seq.fill(100000)(randomCarPrice()).toDS()

        // Join with skewed data
        spark.sparkContext.setJobDescription("Join skewed data")

        val small_df_avg_price = small_df_registrations.join(large_df_prices, Seq("make", "model"))
          // Filter similar vehicles (engine sizes within 0.1 litres of each other)
          .filter(abs(large_df_prices("engine_size") - small_df_registrations("engine_size")) <= BigDecimal("0.1"))
          // Compute average price of similar vehicles
          .groupBy("registration")
          .agg(avg("sale_price").as("average_price"))

        small_df_avg_price.write.format("noop").mode("overwrite").save()

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
          dfJobReportsCnt should be > 1L
        }

        val dfStageReports = spark.read.parquet(parquetSink.stageReportsDirPath)
        val dfStageReportsCnt = dfStageReports.count()
        it("should save Stage reports in parquet file") {
          dfStageReportsCnt should be > 1L
        }

        val dfTaskReports = spark.read.parquet(parquetSink.taskReportsDirPath)
        val dfTaskReportsCnt = dfTaskReports.count()
        it("should save Task reports in parquet file") {
          dfTaskReportsCnt should be > 1L
        }

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
