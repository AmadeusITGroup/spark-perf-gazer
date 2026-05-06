package com.amadeus.perfgazer

import com.amadeus.testfwk.SimpleSpec

class DestinationModeSpec extends SimpleSpec {

  describe("DestinationMode.detect") {

    it("should return Posix for absolute paths") {
      DestinationMode.detect("/tmp/foo") shouldBe DestinationMode.Posix
    }

    it("should return Hdfs for s3://") {
      DestinationMode.detect("s3://bucket/path") shouldBe DestinationMode.Hdfs
    }

    it("should return Hdfs for s3a://") {
      DestinationMode.detect("s3a://bucket/path") shouldBe DestinationMode.Hdfs
    }

    it("should return Hdfs for abfss://") {
      DestinationMode.detect("abfss://container@account/path") shouldBe DestinationMode.Hdfs
    }

    it("should return Hdfs for gs://") {
      DestinationMode.detect("gs://bucket/path") shouldBe DestinationMode.Hdfs
    }

    it("should return Hdfs for dbfs:/") {
      DestinationMode.detect("dbfs:/path") shouldBe DestinationMode.Hdfs
    }

    it("should return Hdfs for hdfs://") {
      DestinationMode.detect("hdfs://namenode/path") shouldBe DestinationMode.Hdfs
    }

    it("should throw IllegalArgumentException for unrecognized scheme") {
      an[IllegalArgumentException] should be thrownBy {
        DestinationMode.detect("ftp://server/path")
      }
    }

    it("should throw IllegalArgumentException for relative path") {
      an[IllegalArgumentException] should be thrownBy {
        DestinationMode.detect("relative/path")
      }
    }
  }
}
