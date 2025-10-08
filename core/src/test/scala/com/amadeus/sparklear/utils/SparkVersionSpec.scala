package com.amadeus.sparklear.utils

import com.amadeus.testfwk.{SimpleSpec, SparkSupport, TempDirSupport}

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

class SparkVersionSpec extends SimpleSpec with SparkSupport {
  describe("Check Spark version") {
    withSpark(appName = this.getClass.getName) { spark =>
      it("it should display the Spark version")  {
        val version = spark.version
        println(s"Spark version: $version")
        version should not be empty
      }
    }
  }
}