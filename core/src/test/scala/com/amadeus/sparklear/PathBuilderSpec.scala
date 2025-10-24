package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, SparkSupport, TempDirSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class PathBuilderSpec extends SimpleSpec with SparkSupport with TempDirSupport {
  describe("Path builder for JSON Sink") {
    withSpark(appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)
        val destination1 = s"$tmpDir"
        it("should build reports destination") {
          destination1 shouldBe s"$tmpDir"
        }

        val destination2 = s"$tmpDir".withDefaultPartitions.resolveProperties(spark.sparkContext.getConf)
        it("should build reports destination (withDefaultPartitions)") {
          destination2 shouldBe tmpDir + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination3 = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id").resolveProperties(spark.sparkContext.getConf)
        it("should build reports destination (withDate / withSparkConf)") {
          destination3 shouldBe tmpDir + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination4 = s"$tmpDir".withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
        it("should build reports destination (withDate / withApplicationId)") {
          destination4 shouldBe tmpDir + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val destination5 = s"$tmpDir"
          .withPartition("customPartition", "myPartition")
          .withDatabricksTag("clusterName", "clusterName")
          .resolveProperties(spark.sparkContext.getConf)
        it("should build reports destination (withPartition / withDatabricksTag)") {
          destination5 shouldBe tmpDir + "/customPartition=myPartition/clusterName=unknown/"
        }
      }
    }
  }
}