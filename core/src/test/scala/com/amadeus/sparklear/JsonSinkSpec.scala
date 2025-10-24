package com.amadeus.sparklear

import com.amadeus.sparklear.JsonSink._
import com.amadeus.sparklear.reports.JobReport
import com.amadeus.testfwk.{SimpleSpec, SinkSupport, TempDirSupport}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import java.io.File
import java.time.Instant

class JsonSinkSpec extends SimpleSpec with TempDirSupport with SinkSupport {
  describe("json sink") {
    val sparkBuilder = SparkSession.builder
      .appName("JsonSinkSpec")
      .master("local[1]")

    it("should write job reports with writeBatchSize = 1") {
      withTmpDir { tmpDir =>
        val sparkConf = new SparkConf()
          .set(JsonSinkDestinationKey, s"$tmpDir")
          .set(JsonSinkWriteBatchSizeKey, "1")
          .set(JsonSinkFileSizeLimitKey, (200L*1024*1024).toString)

        val jsonSink = new JsonSink(sparkConf)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonLocation = new File(s"$tmpDir")
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)

        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should not equal 0

        jsonSink.close()
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should not equal 0

        // check files content
        val spark = sparkBuilder.getOrCreate()
        import spark.implicits._
        val dfJobReports = spark.read.json(s"$tmpDir/job-reports-*.json")
        dfJobReports.count() shouldBe 1
        dfJobReports
          .select("jobId", "groupId", "jobName", "jobStartTime", "jobEndTime", "sqlId", "stages")
          .collectAsList() should equal (List.fill(1)(jr).toDF().collectAsList())
        spark.stop()
      }
    }

    it("should write job reports with writeBatchSize = 5") {
      withTmpDir { tmpDir =>
        val sparkConf = new SparkConf()
          .set(JsonSinkDestinationKey, s"$tmpDir")
          .set(JsonSinkWriteBatchSizeKey, "5")
          .set(JsonSinkFileSizeLimitKey, (200L*1024*1024).toString)
        val jsonSink = new JsonSink(sparkConf)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonLocation = new File(s"$tmpDir")
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)

        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should not equal 0

        // check files content
        val spark = sparkBuilder.getOrCreate()
        import spark.implicits._
        val dfJobReports = spark.read.json(s"$tmpDir/job-reports-*.json")
        dfJobReports.count() shouldBe 5
        dfJobReports
          .select("jobId", "groupId", "jobName", "jobStartTime", "jobEndTime", "sqlId", "stages")
          .collectAsList() should equal (List.fill(5)(jr).toDF().collectAsList())
        spark.stop()
      }
    }
    it("should write job reports when writeBatchSize not reached and sink is flushed") {
      withTmpDir { tmpDir =>
        val sparkConf = new SparkConf()
          .set(JsonSinkDestinationKey, s"$tmpDir")
          .set(JsonSinkWriteBatchSizeKey, "5")
          .set(JsonSinkFileSizeLimitKey, (200L*1024*1024).toString)
        val jsonSink = new JsonSink(sparkConf)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonLocation = new File(s"$tmpDir")
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)

        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)
        jsonSink.write(jr)
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)

        // flush with 4 reports in sink
        jsonSink.close()
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should not equal 0

        // check files content
        val spark = sparkBuilder.getOrCreate()
        import spark.implicits._
        val dfJobReports = spark.read.json(s"$tmpDir/job-reports-*.json")
        dfJobReports.count() shouldBe 4
        dfJobReports
          .select("jobId", "groupId", "jobName", "jobStartTime", "jobEndTime", "sqlId", "stages")
          .collectAsList() should equal (List.fill(4)(jr).toDF().collectAsList())
        spark.stop()
      }
    }
    it("should do file rolling when fileSizeLimit is reached ") {
      withTmpDir { tmpDir =>
        val sparkConf = new SparkConf()
          .set(JsonSinkDestinationKey, s"$tmpDir")
          .set(JsonSinkWriteBatchSizeKey, "100")
          .set(JsonSinkFileSizeLimitKey, (10L*1024).toString)
        val jsonSink = new JsonSink(sparkConf)

        val jr = JobReport(1, "testgroup", "testjob", Instant.now.getEpochSecond, Instant.now.getEpochSecond + 1000, "1", Seq(1))
        val jsonLocation = new File(s"$tmpDir")
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should equal(0)

        for (i <- 1 to 150) {
          jsonSink.write(jr)
        }

        // flush with 50 reports in sink
        jsonSink.close()
        jsonLocation.listFiles().filter(file => file.isFile && file.getName.matches("job-reports-.*\\.json")).map(_.length()).sum should not equal 0

        // check files content
        val spark = sparkBuilder.getOrCreate()
        import spark.implicits._
        val dfJobReports = spark.read.json(s"$tmpDir/job-reports-*.json")
        dfJobReports.count() shouldBe 150
        dfJobReports
          .select("jobId", "groupId", "jobName", "jobStartTime", "jobEndTime", "sqlId", "stages")
          .collectAsList() should equal (List.fill(150)(jr).toDF().collectAsList())
        spark.stop()
      }
    }
    it("should initialize JsonSink from SinkConfig and SparkConf") {
      withTmpDir { tmpDir =>
        val sparkConf = new SparkConf()
          .set(JsonSinkDestinationKey, s"$tmpDir")
          .set(JsonSinkWriteBatchSizeKey, "100")
          .set(JsonSinkFileSizeLimitKey, (10L*1024).toString)
        val jsonSink1 = new JsonSink(sparkConf)

        val jsonSink2 = new JsonSink(
          JsonSink.Config(
            destination = s"$tmpDir",
            writeBatchSize = 100,
            fileSizeLimit = 10L*1024
          ),
          new SparkConf()
        )

        jsonSink1.asString shouldBe jsonSink2.asString
      }
    }
  }
}
