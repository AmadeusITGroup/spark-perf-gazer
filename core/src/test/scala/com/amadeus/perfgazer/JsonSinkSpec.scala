package com.amadeus.perfgazer

import com.amadeus.perfgazer.reports.{Report, ReportType}
import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import com.jayway.jsonpath.JsonPath
import org.apache.spark.SparkConf
import scala.concurrent.{Await, Future}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import java.io.File

class JsonSinkSpec extends SimpleSpec {

  case object DummyReportType extends ReportType {
    override def name: String = "dummy"
  }

  case class DummyReport(id: Int) extends Report {
    override def reportType: ReportType = DummyReportType
  }

  it("should write reports as json in a simple scenario") {
    withTmpDir { tmpDir =>
      val conf = JsonSink.Config(
        destination = s"$tmpDir",
        writeBatchSize = 2,
        fileSizeLimit = 200L * 1024 * 1024
      )
      val jsonSink = new JsonSink(
        config = conf,
        sparkConf = new SparkConf(),
        reportTypes = Set(DummyReportType)
      )

      val jsonLocation = new File(s"$tmpDir")
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0)

      jsonSink.write(DummyReport(1)) // won't flush yet
      jsonSink.write(DummyReport(2))
      jsonSink.write(DummyReport(3))
      jsonSink.close()
      FileUtils.files(jsonLocation).map(_.length()).sum should not equal 0

      // check files content
      val reports = FileUtils.readLines(FileUtils.files(jsonLocation))
      reports.size shouldBe 3
      reports.map(JsonPath.parse(_).read[Int]("$.id")).toSet shouldBe Set(1, 2, 3)
    }
  }

  it("should write job reports on sink flushed (writeBatchSize not reached)") {
    withTmpDir { tmpDir =>
      val conf = JsonSink.Config(
        destination = s"$tmpDir",
        writeBatchSize = 5,
        fileSizeLimit = 200L * 1024 * 1024
      )
      val jsonSink = new JsonSink(
        config = conf,
        sparkConf = new SparkConf(),
        reportTypes = Set(DummyReportType)
      )

      val jsonLocation = new File(s"$tmpDir")
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0)

      jsonSink.write(DummyReport(1))
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0) // won't flush until 5th report or close
      jsonSink.write(DummyReport(2))
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0)
      jsonSink.write(DummyReport(3))
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0)
      jsonSink.write(DummyReport(4))
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0)

      // flush with 4 reports in sink
      jsonSink.close()
      FileUtils.files(jsonLocation).map(_.length()).sum should not equal 0

      // check files content using TestFileUtils
      val reports = FileUtils.readLines(FileUtils.files(jsonLocation))
      reports.size shouldBe 4
      reports.map(JsonPath.parse(_).read[Int]("$.id")).toSet shouldBe Set(1, 2, 3, 4)
    }
  }
  it("should do file rolling when fileSizeLimit is reached") {
    withTmpDir { tmpDir =>
      val conf = JsonSink.Config(
        destination = s"$tmpDir",
        writeBatchSize = 100,
        fileSizeLimit = 1L * 1024 // each record around 10 bytes, ~100 records per file
      )
      val jsonSink = new JsonSink(
        config = conf,
        sparkConf = new SparkConf(),
        reportTypes = Set(DummyReportType)
      )

      val jsonLocation = new File(s"$tmpDir")
      FileUtils.files(jsonLocation).map(_.length()).sum should equal(0)

      for (id <- 1 to 150) {
        jsonSink.write(DummyReport(id))
      }

      // flush with 50 reports in sink
      jsonSink.close()
      FileUtils.files(jsonLocation).size shouldBe >(1)
      FileUtils.files(jsonLocation).map(_.length()).sum should not be 0

      // check files content using TestFileUtils
      val reports = FileUtils.readLines(FileUtils.files(jsonLocation))
      reports.size shouldBe 150
      reports.map(JsonPath.parse(_).read[Int]("$.id")).toSet shouldBe (1 to 150).toSet
    }
  }
  it("should initialize JsonSink from SparkConf") {
    withTmpDir { tmpDir =>
      val sparkConf = new SparkConf()
        .set(JsonSink.DestinationKey, s"$tmpDir")
        .set(JsonSink.WriteBatchSizeKey, "100")
        .set(JsonSink.FileSizeLimitKey, (10L * 1024).toString)
      val jsonSink1 = new JsonSink(sparkConf)

      val jsonSink2 = new JsonSink(
        JsonSink.Config(
          destination = s"$tmpDir",
          writeBatchSize = 100,
          fileSizeLimit = 10L * 1024
        ),
        new SparkConf()
      )

      jsonSink1.description shouldBe jsonSink2.description
    }
  }
  it("should initialize JsonSink from SparkConf using default values") {
    withTmpDir { tmpDir =>
      val sparkConf = new SparkConf().set(JsonSink.DestinationKey, s"$tmpDir")
      val jsonSink1 = new JsonSink(sparkConf)
      val jsonSink2 = new JsonSink(JsonSink.Config(destination = s"$tmpDir"), new SparkConf())
      jsonSink1.description shouldBe jsonSink2.description
    }
  }
  it("should fail to initialize JsonSink from empty SparkConf - missing destination") {
    val sparkConf = new SparkConf()
    an[IllegalArgumentException] should be thrownBy {
      new JsonSink(sparkConf)
    }
  }

  it("should handle concurrent writes from multiple threads without data loss or corruption") {
    withTmpDir { tmpDir =>
      val conf = JsonSink.Config(
        destination = s"$tmpDir",
        writeBatchSize = 1,
        fileSizeLimit = 200L * 1024 * 1024
      )
      val jsonSink = new JsonSink(
        config = conf,
        sparkConf = new SparkConf(),
        reportTypes = Set(DummyReportType)
      )
      val jsonLocation = new File(s"$tmpDir")
      val n = 500
      val futures = (1 to n).map { i =>
        Future {
          jsonSink.write(DummyReport(i))
        }
      }
      Await.result(Future.sequence(futures), 10.seconds) // wait for all writes to complete
      jsonSink.close()
      val reports = FileUtils.readLines(FileUtils.files(jsonLocation))
      reports.size shouldBe n
      reports.map(JsonPath.parse(_).read[Int]("$.id")).toSet shouldBe (1 to n).toSet
    }
  }

  it("should not write after close (write after close is ignored)") {
    withTmpDir { tmpDir =>
      val conf = JsonSink.Config(
        destination = s"$tmpDir",
        writeBatchSize = 1,
        fileSizeLimit = 200L * 1024 * 1024
      )
      val jsonSink = new JsonSink(
        config = conf,
        sparkConf = new SparkConf(),
        reportTypes = Set(DummyReportType)
      )
      val jsonLocation = new File(s"$tmpDir")
      jsonSink.write(DummyReport(1))
      jsonSink.close()

      jsonSink.write(DummyReport(2)) // will be ignored

      val reports = FileUtils.readLines(FileUtils.files(jsonLocation))
      reports.map(JsonPath.parse(_).read[Int]("$.id")).toSet should contain(1) // only contains 1
    }
  }

  it("should allow multiple close() calls from different threads without error") {
    withTmpDir { tmpDir =>
      val conf = JsonSink.Config(
        destination = s"$tmpDir",
        writeBatchSize = 1,
        fileSizeLimit = 200L * 1024 * 1024
      )
      val jsonSink = new JsonSink(
        config = conf,
        sparkConf = new SparkConf(),
        reportTypes = Set(DummyReportType)
      )
      val closes = (1 to 10).map(_ =>
        Future {
          jsonSink.close()
        }
      )
      Await.result(Future.sequence(closes), 10.seconds)
      // No exception should be thrown
      succeed
    }
  }

}
