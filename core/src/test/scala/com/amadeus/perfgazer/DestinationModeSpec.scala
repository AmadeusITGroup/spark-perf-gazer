package com.amadeus.perfgazer

import com.amadeus.testfwk.SimpleSpec

import java.net.URISyntaxException
import scala.util.Success

class DestinationModeSpec extends SimpleSpec {

  describe("DestinationMode.detect") {

    it("should return Posix for absolute paths (no scheme)") {
      DestinationMode.detect("/tmp/foo") shouldBe Success(DestinationMode.Posix)
    }

    it("should return Posix for scheme-less relative paths") {
      DestinationMode.detect("relative/path") shouldBe Success(DestinationMode.Posix)
    }

    it("should return Hdfs for s3://") {
      DestinationMode.detect("s3://bucket/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for s3a://") {
      DestinationMode.detect("s3a://bucket/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for abfss://") {
      DestinationMode.detect("abfss://container@account/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for gs://") {
      DestinationMode.detect("gs://bucket/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for dbfs:/") {
      DestinationMode.detect("dbfs:/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for hdfs://") {
      DestinationMode.detect("hdfs://namenode/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for any other scheme (Hadoop decides actual support)") {
      DestinationMode.detect("ftp://server/path") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Hdfs for file://") {
      DestinationMode.detect("file:///tmp/reports") shouldBe Success(DestinationMode.Hdfs)
    }

    it("should return Posix for a Windows path with a lowercase drive letter") {
      DestinationMode.detect("c:/tmp/reports") shouldBe Success(DestinationMode.Posix)
    }

    it("should return Posix for a Windows path with a drive letter and forward slashes") {
      DestinationMode.detect("C:/tmp/reports") shouldBe Success(DestinationMode.Posix)
    }

    it("should return Posix for a Windows path with backslashes") {
      DestinationMode.detect("C:\\tmp\\reports") shouldBe Success(DestinationMode.Posix)
    }

    it("should return a Failure for a malformed destination") {
      val result = DestinationMode.detect("http://exa mple.com/path")
      result.isFailure shouldBe true
      result.failed.get shouldBe a[URISyntaxException]
    }

    it("should return a Failure for an empty destination") {
      val result = DestinationMode.detect("")
      result.isFailure shouldBe true
      result.failed.get shouldBe a[IllegalArgumentException]
    }
  }
}
