package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, SparkSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class PathBuilderSpec extends SimpleSpec with SparkSupport {
  describe("Path builder for JSON Sink") {
    withSpark(appName = this.getClass.getName) { spark =>
      val tmpDirUnix: String = "/tmp/sparklear/pathbuilder/spec"
      val tmpDirWin: String = "C:\\tmp\\sparklear\\pathbuilder\\spec"

      val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)

      val destination2Unix = tmpDirUnix.withDefaultPartitions.resolveProperties(spark.sparkContext.getConf)
      val destination2Win = tmpDirWin.withDefaultPartitions.resolveProperties(spark.sparkContext.getConf)
      it("should build reports destination (withDefaultPartitions)") {
        destination2Unix shouldBe tmpDirUnix + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        destination2Win shouldBe tmpDirWin + s"\\date=$currentDate\\applicationId=${spark.sparkContext.applicationId}\\"
      }

      val destination3Unix = tmpDirUnix.withDate.withSparkConf("applicationId", "spark.app.id").resolveProperties(spark.sparkContext.getConf)
      val destination3Win = tmpDirWin.withDate.withSparkConf("applicationId", "spark.app.id").resolveProperties(spark.sparkContext.getConf)
      it("should build reports destination (withDate / withSparkConf)") {
        destination3Unix shouldBe tmpDirUnix + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        destination3Win shouldBe tmpDirWin + s"\\date=$currentDate\\applicationId=${spark.sparkContext.applicationId}\\"
      }

      val destination4Unix = tmpDirUnix.withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
      val destination4Win = tmpDirWin.withDate.withApplicationId.resolveProperties(spark.sparkContext.getConf)
      it("should build reports destination (withDate / withApplicationId)") {
        destination4Unix shouldBe tmpDirUnix + s"/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        destination4Win shouldBe tmpDirWin + s"\\date=$currentDate\\applicationId=${spark.sparkContext.applicationId}\\"
      }

      val destination5 = tmpDirUnix
        .withPartition("customPartition", "myPartition")
        .withDatabricksTag("clusterName", "clusterName")

      it("should throw IllegalArgumentException if one of partition value cannot be resolved") {
        an[IllegalArgumentException] should be thrownBy {
          destination5.resolveProperties(spark.sparkContext.getConf)
        }
      }

      it("should throw IllegalArgumentException if one of partition key contains invalid characters") {
        an[IllegalArgumentException] should be thrownBy {
          tmpDirUnix.withPartition("custom=Partition", "myPartition")
        }
      }

      it("should throw IllegalArgumentException if one of partition value contains invalid characters") {
        an[IllegalArgumentException] should be thrownBy {
          tmpDirUnix.withPartition("customPartition", "my=Partition")
        }
      }

      it("should handle a simple path with no ending /") {
        val path = "/tmp"
        path.withWildcards() shouldBe "/tmp/"
        path.extractBasePath() shouldBe "/tmp/"
        path.extractPartitions() shouldBe ""
      }

      it("should handle a simple path with intermediate / and no partitions") {
        val path = "/tmp/listener"
        path.withWildcards() shouldBe "/tmp/listener/"
        path.extractBasePath() shouldBe "/tmp/listener/"
        path.extractPartitions() shouldBe ""
      }

      it("should handle a path with one partition segment") {
        val path = "/tmp/listener/date=2025-09-10"
        path.withWildcards() shouldBe "/tmp/listener/date=*/"
        path.extractBasePath() shouldBe "/tmp/listener/"
        path.extractPartitions() shouldBe "/date=2025-09-10/"
      }

      it("should handle a path with multiple partition segments") {
        val path = "/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg"
        path.withWildcards() shouldBe "/tmp/listener/date=*/cluster=*/id=*/level=*/"
        path.extractBasePath() shouldBe "/tmp/listener/"
        path.extractPartitions() shouldBe "/date=2025-09-10/cluster=111/id=ffff/level=ggg/"
      }

      it("should handle a path with only partition segments after base") {
        val path = "/base/a=10/b=20/c=30/"
        path.withWildcards() shouldBe "/base/a=*/b=*/c=*/"
        path.extractBasePath() shouldBe "/base/"
        path.extractPartitions() shouldBe "/a=10/b=20/c=30/"
      }

      it("should handle a path with non-partition segments between partitions") {
        val path = "/base/a=10/something/b=10/c=30"
        path.withWildcards() shouldBe "/base/a=*/something/b=*/c=*/"
        path.extractBasePath() shouldBe "/base/a=10/something/"
        path.extractPartitions() shouldBe "/b=10/c=30/"
      }
    }
  }
}