package com.amadeus.sparklear

import com.amadeus.sparklear.PathBuilder.PathOps
import com.amadeus.testfwk.{SimpleSpec, SparkSupport, TempDirSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class JsonSinkConfigSpec extends SimpleSpec with SparkSupport with TempDirSupport {
  describe("config builder for JSON Sink") {
    withSpark(appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        val currentDate = LocalDateTime.now().format(DateTimeFormatter.ISO_DATE)

        val c1 = JsonSinkConfig.build(destination = s"$tmpDir")
        val s1 = new JsonSink(c1)
        val reportsDestination1 = s1.config.destination

        it("should build reports destination") {
          reportsDestination1 shouldBe s"$tmpDir/"
        }

        val c2 = JsonSinkConfig.build(destination = s"$tmpDir".withDefaultPartitions(spark.conf.getAll))
        val s2 = new JsonSink(c2)
        val reportsDestination2 = s2.config.destination

        it("should build reports destination (withDefaultPartitions)") {
          reportsDestination2 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c3 = JsonSinkConfig.build(destination = s"$tmpDir".withDate.withSparkConf("applicationId", "spark.app.id", spark.conf.getAll))
        val s3 = new JsonSink(c3)
        val reportsDestination3 = s3.config.destination

        it("should build reports destination (withDate / withSparkConf)") {
          reportsDestination3 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c4 = JsonSinkConfig.build(destination = s"$tmpDir".withDate.withApplicationId(spark.conf.getAll))
        val s4 = new JsonSink(c4)
        val reportsDestination4 = s4.config.destination

        it("should build reports destination (withDate / withApplicationId)") {
          reportsDestination4 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c5 = JsonSinkConfig.build(destination = s"$tmpDir".withDate(DateTimeFormatter.ISO_DATE).withApplicationId(spark.conf.getAll))
        val s5 = new JsonSink(c5)
        val reportsDestination5 = s5.config.destination

        it("should build reports destination (withDate(DateTimeFormatter) / withApplicationId)") {
          reportsDestination5 shouldBe s"$tmpDir/date=$currentDate/applicationId=${spark.sparkContext.applicationId}/"
        }

        val c6 = JsonSinkConfig.build(
          destination = s"$tmpDir"
            .withPartition("customPartition", "myPartition")
            .withDatabricksTag("clusterName", "clusterName", spark.conf.getAll))
        val s6 = new JsonSink(c6)
        val reportsDestination6 = s6.config.destination

        it("should build reports destination (withPartition / withDatabricksTag)") {
          reportsDestination6 shouldBe s"$tmpDir/customPartition=myPartition/clusterName=unknown/"
        }
      }
    }
  }
}