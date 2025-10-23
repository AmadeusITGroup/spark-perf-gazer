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

        val destination5 = s"$tmpDir".withDateCustom("yyyy-MM-dd").withApplicationId(spark.conf.getAll)
        it("should build reports destination (withDate(DateTimeFormatter) / withApplicationId)") {
          destination5 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination6 = s"$tmpDir"
          .withPartition("customPartition", "myPartition")
          .withDatabricksTag("clusterName", "clusterName", spark.conf.getAll)
        it("should build reports destination (withPartition / withDatabricksTag)") {
          destination6 shouldBe s"$tmpDir/customPartition=myPartition/clusterName=unknown/"
        }

        val destination12 = s"$tmpDir".invokePathOpsMethods("withDefaultPartitions", spark.conf.getAll)
        it("should build reports destination invokePathOps(\"withDefaultPartitions\")") {
          destination12 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination13 = s"$tmpDir".invokePathOpsMethods("withDate;withSparkConf:applicationId,spark.app.id", spark.conf.getAll)
        it("should build reports destination invokePathOps(\"withDate;withSparkConf:applicationId,spark.app.id\")") {
          destination13 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination14 = s"$tmpDir".invokePathOpsMethods("withDate;withApplicationId", spark.conf.getAll)
        it("should build reports destination invokePathOps(\"withDate;withApplicationId\")") {
          destination14 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination15 = s"$tmpDir".invokePathOpsMethods("withDateCustom:yyyy-MM-dd;withApplicationId", spark.conf.getAll)
        it("should build reports destination invokePathOps(\"withDateCustom:yyyy-MM-dd;withApplicationId\")") {
          destination15 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination16 = s"$tmpDir".invokePathOpsMethods("withPartition:customPartition,myPartition;withDatabricksTag:clusterName,clusterName", spark.conf.getAll)
        it("should build reports destination invokePathOps(\"withPartition:customPartition,myPartition;withDatabricksTag:clusterName,clusterName\")") {
          destination16 shouldBe s"$tmpDir/customPartition=myPartition/clusterName=unknown/"
        }
      }
    }
  }
}