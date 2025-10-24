package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, TempDirSupport}

class PathBuilderSpec extends SimpleSpec with TempDirSupport {
  describe("path builder for JSON Sink") {
    withTmpDir { tmpDir =>
      val destination1 = s"$tmpDir"
      it("should build reports destination") {
        destination1 shouldBe s"$tmpDir"
      }

      val destination2 = s"$tmpDir".withDefaultPartitions
      it("should build reports destination (withDefaultPartitions)") {
        destination2 shouldBe tmpDir + "/date=${sparklear.now.year}-${sparklear.now.month}-${sparklear.now.day}/applicationId=${spark.app.id}/"
      }

      val destination3 = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id")
      it("should build reports destination (withDate / withSparkConf)") {
        destination3 shouldBe tmpDir + "/date=${sparklear.now.year}-${sparklear.now.month}-${sparklear.now.day}/applicationId=${spark.app.id}/"
      }

      val destination4 = s"$tmpDir".withDate.withApplicationId
      it("should build reports destination (withDate / withApplicationId)") {
        destination4 shouldBe tmpDir + "/date=${sparklear.now.year}-${sparklear.now.month}-${sparklear.now.day}/applicationId=${spark.app.id}/"
      }

      val destination5 = s"$tmpDir"
        .withPartition("customPartition", "myPartition")
        .withDatabricksTag("clusterName", "clusterName")
      it("should build reports destination (withPartition / withDatabricksTag)") {
        destination5 shouldBe tmpDir + "/customPartition=myPartition/clusterName=${spark.databricks.clusterUsageTags.clusterName}/"
      }
    }
  }
}