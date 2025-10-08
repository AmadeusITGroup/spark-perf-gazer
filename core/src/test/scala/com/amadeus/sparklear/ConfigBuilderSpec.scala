package com.amadeus.sparklear

import com.amadeus.sparklear.ConfigBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, SparkSupport, TempDirSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class ConfigBuilderSpec extends SimpleSpec with SparkSupport with TempDirSupport {
  describe("config builder for JSON Sink") {
    withSpark(appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        it("should build default config with LogSink") {
          val cDefault = ConfigBuilder.buildConf(spark.conf.getAll, InputParams())
          cDefault.sink.getClass.getCanonicalName shouldBe s"com.amadeus.sparklear.LogSink"
        }

        it("should build config with LogSink when unknown listenerSinkType is provided") {
          val cUnknown = ConfigBuilder.buildConf(spark.conf.getAll, InputParams(listenerSinkType = "Unknown"))
          cUnknown.sink.getClass.getCanonicalName shouldBe s"com.amadeus.sparklear.LogSink"
        }

        val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)

        val p1 = InputParams(listenerSinkType = "Json", listenerOutputBaseDir = s"$tmpDir")
        val c1 = ConfigBuilder.buildConf(spark.conf.getAll, p1)
        val reportsDestination1 = c1.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination1)

        it("should build reports destination with default listenerOutputPartitions") {
          reportsDestination1 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p2 = InputParams(
          listenerSinkType = "Json",
          listenerOutputBaseDir = s"$tmpDir",
          listenerOutputPartitions = Map(
            "customPartition" -> "myPartition",
            "applicationName" -> "spark.app.name",
            "clusterName"-> "spark.databricks.clusterUsageTags.clusterName"
          )
        )
        val c2 = ConfigBuilder.buildConf(spark.conf.getAll, p2)
        val reportsDestination2 = c2.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination2)

        it("should build reports destination with custom listenerOutputPartitions") {
          reportsDestination2 shouldBe s"$tmpDir/customPartition=myPartition/applicationName=${spark.sparkContext.appName}/clusterName=unknown/"
        }

        val p3 = InputParams(
          listenerSinkType = "Json",
          listenerOutputBaseDir = s"$tmpDir",
          listenerOutputPartitions = Map.empty
        )
        val c3 = ConfigBuilder.buildConf(spark.conf.getAll, p3)
        val reportsDestination3 = c3.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination3)

        it("should build reports destination with empty listenerOutputPartitions") {
          reportsDestination3 shouldBe s"$tmpDir/"
        }

        val p4 = InputParams(
          listenerSinkType = "Json",
          listenerOutputBaseDir = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id", spark.conf.getAll),
          listenerOutputPartitions = Map.empty
        )
        val c4 = ConfigBuilder.buildConf(spark.conf.getAll, p4)
        val reportsDestination4 = c4.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination4)

        it("should build reports destination with PathOps #1") {
          reportsDestination4 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p5 = InputParams(
          listenerSinkType = "Json",
          listenerOutputBaseDir = s"$tmpDir".withDate.withApplicationId(spark.conf.getAll),
          listenerOutputPartitions = Map.empty
        )
        val c5 = ConfigBuilder.buildConf(spark.conf.getAll, p5)
        val reportsDestination5 = c5.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination5)

        it("should build reports destination with PathOps #2") {
          reportsDestination5 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p6 = InputParams(
          listenerSinkType = "Json",
          listenerOutputBaseDir = s"$tmpDir".withDate(DateTimeFormatter.ISO_DATE).withApplicationId(spark.conf.getAll),
          listenerOutputPartitions = Map.empty
        )
        val c6 = ConfigBuilder.buildConf(spark.conf.getAll, p6)
        val reportsDestination6 = c6.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination6)

        it("should build reports destination with PathOps #3") {
          reportsDestination6 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p7 = InputParams(
          listenerSinkType = "Json",
          listenerOutputBaseDir = s"$tmpDir"
            .withPartition("customPartition", "myPartition")
            .withDatabricksTag("clusterName", "clusterName", spark.conf.getAll),
          listenerOutputPartitions = Map.empty
        )
        val c7 = ConfigBuilder.buildConf(spark.conf.getAll, p7)
        val reportsDestination7 = c7.sink.asInstanceOf[JsonSink].destination
        println(reportsDestination7)

        it("should build reports destination with PathOps #4") {
          reportsDestination7 shouldBe s"$tmpDir/customPartition=myPartition/clusterName=unknown/"
        }
      }
    }
  }
}