package com.amadeus.perfgazer

import com.amadeus.perfgazer.PathBuilder.PathOps
import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.SparkSupport.withSpark
import org.scalatest.GivenWhenThen

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class PathBuilderSpec extends SimpleSpec with GivenWhenThen {
  describe("Path builder for JSON Sink") {
    it("should build reports destinations using Spark properties") {
      withSpark(appName = this.getClass.getName) { spark =>
        Given("path templates and a Spark session")
        val tmpDirUnix: String = "/tmp/perfgazer/pathbuilder/spec"
        val tmpDirWin: String = "C:\\tmp\\perfgazer\\pathbuilder\\spec"
        val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)

        Then("it should build reports destination (withDefaultPartitions)")
        val destination2Unix = tmpDirUnix.withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
        val destination2Win = tmpDirWin.withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
        destination2Unix shouldBe tmpDirUnix + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        destination2Win shouldBe tmpDirWin + s"\\date=$currentDate\\applicationId=${spark.sparkContext.applicationId}\\"

        And("it should build reports destination (withDate / withSparkConf)")
        val destination3Unix = tmpDirUnix.withDate.withSparkConf("applicationId", "spark.app.id").resolveProperties(spark.sparkContext.getConf)
        val destination3Win = tmpDirWin.withDate.withSparkConf("applicationId", "spark.app.id").resolveProperties(spark.sparkContext.getConf)
        destination3Unix shouldBe tmpDirUnix + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        destination3Win shouldBe tmpDirWin + s"\\date=$currentDate\\applicationId=${spark.sparkContext.applicationId}\\"

        And("it should build reports destination (withDate / withApplicationId)")
        val destination4Unix = tmpDirUnix.withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
        val destination4Win = tmpDirWin.withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
        destination4Unix shouldBe tmpDirUnix + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        destination4Win shouldBe tmpDirWin + s"\\date=$currentDate\\applicationId=${spark.sparkContext.applicationId}\\"

        And("it should resolve perfgazer.runid with the run id/uuid")
        val fixedUuid = "00000000-0000-0000-0000-000000000042"
        val destinationWithRunId = tmpDirUnix.withRunId.resolveProperties(
          spark.sparkContext.getConf,
          () => java.util.UUID.fromString(fixedUuid)
        )
        destinationWithRunId shouldBe tmpDirUnix + s"/runId=$fixedUuid/"

        And("it should generate a the same uuid for a given runtime")
        val uuid1 = tmpDirUnix.withRunId.resolveProperties(spark.sparkContext.getConf)
        val uuid2 = tmpDirUnix.withRunId.resolveProperties(spark.sparkContext.getConf)
        uuid1 shouldEqual uuid2

        And("it should throw IllegalArgumentException if one of partition value cannot be resolved")
        val destination5 = tmpDirUnix
          .withPartition("customPartition", "myPartition")
          .withDatabricksTag("clusterName", "clusterName")
        an[IllegalArgumentException] should be thrownBy {
          destination5.resolveProperties(spark.sparkContext.getConf)
        }
      }
    }

    it("should throw IllegalArgumentException if one of partition key contains invalid characters") {
      an[IllegalArgumentException] should be thrownBy {
        "/tmp/perfgazer/pathbuilder/spec".withPartition("custom=Partition", "myPartition")
      }
    }

    it("should throw IllegalArgumentException if one of partition value contains invalid characters") {
      an[IllegalArgumentException] should be thrownBy {
        "/tmp/perfgazer/pathbuilder/spec".withPartition("customPartition", "my=Partition")
      }
    }

    it("should handle a simple path with no ending /") {
      val path = "/tmp"
      path.extractBasePath shouldBe "/tmp/"
      path.extractPartitions shouldBe ""
    }

    it("should handle a simple path with intermediate / and no partitions") {
      val path = "/tmp/listener"
      path.extractBasePath shouldBe "/tmp/listener/"
      path.extractPartitions shouldBe ""
    }

    it("should handle a path with one partition segment") {
      val path = "/tmp/listener/date=2025-09-10"
      path.extractPartitions.globPathValues shouldBe "/date=*/"
      path.extractBasePath shouldBe "/tmp/listener/"
      path.extractPartitions shouldBe "/date=2025-09-10/"
    }

    it("should handle a path with multiple partition segments") {
      val path = "/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg"
      path.extractPartitions.globPathValues shouldBe "/date=*/cluster=*/id=*/level=*/"
      path.extractBasePath shouldBe "/tmp/listener/"
      path.extractPartitions shouldBe "/date=2025-09-10/cluster=111/id=ffff/level=ggg/"
    }

    it("should handle a path with only partition segments after base") {
      val path = "/base/a=10/b=20/c=30/"
      path.extractPartitions.globPathValues shouldBe "/a=*/b=*/c=*/"
      path.extractBasePath shouldBe "/base/"
      path.extractPartitions shouldBe "/a=10/b=20/c=30/"
    }

    it("should handle a path with non-partition segments between partitions") {
      val path = "/base/a=10/something/b=10/c=30"
      path.extractPartitions.globPathValues shouldBe "/b=*/c=*/"
      path.extractBasePath shouldBe "/base/a=10/something/"
      path.extractPartitions shouldBe "/b=10/c=30/"
    }
  }
}
