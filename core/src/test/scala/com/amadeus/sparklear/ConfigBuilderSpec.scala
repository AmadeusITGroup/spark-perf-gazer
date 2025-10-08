package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, SparkSupport, TempDirSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class ConfigBuilderSpec extends SimpleSpec with SparkSupport with TempDirSupport {
  describe("config builder for JSON Sink") {
    withSpark(appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)

        val c1 = JsonSinkConfig.build(spark.conf.getAll, JsonSinkConfigParams(outputBaseDir = s"$tmpDir"))
        val s1 = new JsonSink(c1)

        val reportsDestination1 = s1.config.destination
        println(reportsDestination1)

        it("should build reports destination with default outputPartitions") {
          reportsDestination1 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p2 = JsonSinkConfigParams(
          outputBaseDir = s"$tmpDir",
          outputPartitions = Map(
            "customPartition" -> "myPartition",
            "applicationName" -> "spark.app.name",
            "clusterName"-> "spark.databricks.clusterUsageTags.clusterName"
          )
        )
        val c2 = JsonSinkConfig.build(spark.conf.getAll, p2)
        val s2 = new JsonSink(c2)

        val reportsDestination2 = s2.config.destination
        println(reportsDestination2)

        it("should build reports destination with custom outputPartitions") {
          reportsDestination2 shouldBe s"$tmpDir/customPartition=myPartition/applicationName=${spark.sparkContext.appName}/clusterName=unknown/"
        }

        val p3 = JsonSinkConfigParams(
          outputBaseDir = s"$tmpDir",
          outputPartitions = Map.empty
        )
        val c3 = JsonSinkConfig.build(spark.conf.getAll, p3)
        val s3 = new JsonSink(c3)
        val reportsDestination3 = s3.config.destination
        println(reportsDestination3)

        it("should build reports destination with empty outputPartitions") {
          reportsDestination3 shouldBe s"$tmpDir/"
        }

        val p4 = JsonSinkConfigParams(
          outputBaseDir = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id", spark.conf.getAll),
          outputPartitions = Map.empty
        )
        val c4 = JsonSinkConfig.build(spark.conf.getAll, p4)
        val s4 = new JsonSink(c4)
        val reportsDestination4 = s4.config.destination
        println(reportsDestination4)

        it("should build reports destination with PathOps #1") {
          reportsDestination4 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p5 = JsonSinkConfigParams(
          outputBaseDir = s"$tmpDir".withDate.withApplicationId(spark.conf.getAll),
          outputPartitions = Map.empty
        )
        val c5 = JsonSinkConfig.build(spark.conf.getAll, p5)
        val s5 = new JsonSink(c5)
        val reportsDestination5 = s5.config.destination
        println(reportsDestination5)

        it("should build reports destination with PathOps #2") {
          reportsDestination5 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p6 = JsonSinkConfigParams(
          outputBaseDir = s"$tmpDir".withDate(DateTimeFormatter.ISO_DATE).withApplicationId(spark.conf.getAll),
          outputPartitions = Map.empty
        )
        val c6 = JsonSinkConfig.build(spark.conf.getAll, p6)
        val s6 = new JsonSink(c6)
        val reportsDestination6 = s6.config.destination
        println(reportsDestination6)

        it("should build reports destination with PathOps #3") {
          reportsDestination6 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val p7 = JsonSinkConfigParams(
          outputBaseDir = s"$tmpDir"
            .withPartition("customPartition", "myPartition")
            .withDatabricksTag("clusterName", "clusterName", spark.conf.getAll),
          outputPartitions = Map.empty
        )
        val c7 = JsonSinkConfig.build(spark.conf.getAll, p7)
        val s7 = new JsonSink(c7)
        val reportsDestination7 = s7.config.destination
        println(reportsDestination7)

        it("should build reports destination with PathOps #4") {
          reportsDestination7 shouldBe s"$tmpDir/customPartition=myPartition/clusterName=unknown/"
        }
      }
    }
  }
}