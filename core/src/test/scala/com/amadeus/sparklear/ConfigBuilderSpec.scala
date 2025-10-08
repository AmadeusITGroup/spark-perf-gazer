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

        val c1 = JsonSinkConfig.withBaseDir(sparkConf = spark.conf.getAll, baseDir = s"$tmpDir")
        val s1 = new JsonSink(c1)

        val reportsDestination1 = s1.config.destination
        println(reportsDestination1)

        it("should build reports destination with default outputPartitions") {
          reportsDestination1 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c3 = JsonSinkConfig.withDestination(
          destination = s"$tmpDir"
        )
        val s3 = new JsonSink(c3)
        val reportsDestination3 = s3.config.destination
        println(reportsDestination3)

        it("should build reports destination with empty outputPartitions") {
          reportsDestination3 shouldBe s"$tmpDir/"
        }

        val c4 = JsonSinkConfig.withDestination(
          destination = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id", spark.conf.getAll)
        )
        val s4 = new JsonSink(c4)
        val reportsDestination4 = s4.config.destination
        println(reportsDestination4)

        it("should build reports destination with PathOps #1") {
          reportsDestination4 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c5 = JsonSinkConfig.withDestination(
          destination = s"$tmpDir".withDate.withApplicationId(spark.conf.getAll)
        )
        val s5 = new JsonSink(c5)
        val reportsDestination5 = s5.config.destination
        println(reportsDestination5)

        it("should build reports destination with PathOps #2") {
          reportsDestination5 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c6 = JsonSinkConfig.withDestination(
          destination = s"$tmpDir".withDate(DateTimeFormatter.ISO_DATE).withApplicationId(spark.conf.getAll)
        )
        val s6 = new JsonSink(c6)
        val reportsDestination6 = s6.config.destination
        println(reportsDestination6)

        it("should build reports destination with PathOps #3") {
          reportsDestination6 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c7 = JsonSinkConfig.withDestination(
          destination = s"$tmpDir"
            .withPartition("customPartition", "myPartition")
            .withDatabricksTag("clusterName", "clusterName", spark.conf.getAll))
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