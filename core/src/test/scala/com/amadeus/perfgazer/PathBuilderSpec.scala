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
        val destination2Unix = tmpDirUnix.withDefaultPartitions.resolveProperties(spark.sparkContext.getConf)
        val destination2Win = tmpDirWin.withDefaultPartitions.resolveProperties(spark.sparkContext.getConf)
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

        And("it should throw IllegalArgumentException if one of partition value cannot be resolved")
        val destination5 = tmpDirUnix
          .withPartition("customPartition", "myPartition")
          .withDatabricksTag("clusterName", "clusterName")
        an[IllegalArgumentException] should be thrownBy {
          destination5.resolveProperties(spark.sparkContext.getConf)
        }

        And("it should resolve to the same value when called multiple times (JVM-stable time and UUID)")
        val conf = spark.sparkContext.getConf
        val template = "/tmp/perfgazer".withDate.withTime.withRunId

        val first = template.resolveProperties(conf)
        Thread.sleep(2000)
        val second = template.resolveProperties(conf)
        first shouldBe second
        first should include("runId=")
        first should include("date=")
        first should include("time=")

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
  }

  describe("normalizePath") {
    it("should ensure a Unix path ends with a separator") {
      "/tmp/a/b".normalizePath shouldBe "/tmp/a/b/"
    }

    it("should not duplicate trailing separator") {
      "/tmp/a/b/".normalizePath shouldBe "/tmp/a/b/"
    }

    it("should collapse multiple forward slashes") {
      "/tmp//a///b".normalizePath shouldBe "/tmp/a/b/"
    }

    it("should normalize a Windows path using backslash") {
      "C:\\tmp\\a\\b".normalizePath shouldBe "C:\\tmp\\a\\b\\"
    }

    it("should collapse multiple backslashes") {
      "C:\\\\tmp\\\\a".normalizePath shouldBe "C:\\tmp\\a\\"
    }

    it("should return the input unchanged when no separators are present") {
      "noseparator".normalizePath shouldBe "noseparator"
    }

    it("should preserve URI scheme double-slash for abfss://") {
      "abfss://container@account.blob.core.windows.net/path/to/dir".normalizePath shouldBe
        "abfss://container@account.blob.core.windows.net/path/to/dir/"
    }

    it("should preserve URI scheme double-slash for s3a://") {
      "s3a://bucket/prefix/path".normalizePath shouldBe "s3a://bucket/prefix/path/"
    }

    it("should preserve URI scheme double-slash for hdfs://") {
      "hdfs://namenode:8020/data/output".normalizePath shouldBe "hdfs://namenode:8020/data/output/"
    }

    it("should collapse multiple slashes in path portion of URI but preserve scheme") {
      "abfss://container@account.blob.core.windows.net//path///to//dir".normalizePath shouldBe
        "abfss://container@account.blob.core.windows.net/path/to/dir/"
    }

    it("should preserve URI scheme for gs://") {
      "gs://bucket/path/to/dir".normalizePath shouldBe "gs://bucket/path/to/dir/"
    }

    it("should preserve URI scheme for s3://") {
      "s3://bucket/path".normalizePath shouldBe "s3://bucket/path/"
    }

  }
}
