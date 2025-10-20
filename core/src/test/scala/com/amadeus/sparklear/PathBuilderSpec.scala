package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, SparkSupport, TempDirSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class PathBuilderSpec extends SimpleSpec with SparkSupport with TempDirSupport {
  describe("config builder for JSON Sink") {
    withSpark(appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)

        val destination = s"$tmpDir/"
        it("should build reports destination") {
          destination shouldBe s"$tmpDir/"
        }

        val destination2 = s"$tmpDir".withDefaultPartitions(spark.conf.getAll)
        it("should build reports destination (withDefaultPartitions)") {
          destination2 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination3 = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id", spark.conf.getAll)
        it("should build reports destination (withDate / withSparkConf)") {
          destination3 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination4 = s"$tmpDir".withDate.withApplicationId(spark.conf.getAll)
        it("should build reports destination (withDate / withApplicationId)") {
          destination4 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination5 = s"$tmpDir".withDate(DateTimeFormatter.ISO_DATE).withApplicationId(spark.conf.getAll)

        it("should build reports destination (withDate(DateTimeFormatter) / withApplicationId)") {
          destination5 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination6 = s"$tmpDir"
            .withPartition("customPartition", "myPartition")
            .withDatabricksTag("clusterName", "clusterName", spark.conf.getAll)

        it("should build reports destination (withPartition / withDatabricksTag)") {
          destination6 shouldBe s"$tmpDir/customPartition=myPartition/clusterName=unknown/"
        }
      }
    }
  }
}