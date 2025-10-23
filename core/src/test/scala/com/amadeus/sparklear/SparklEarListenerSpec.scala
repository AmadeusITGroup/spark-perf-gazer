package com.amadeus.sparklear

import com.amadeus.sparklear.fixtures.Fixtures
import com.amadeus.sparklear.utils.CappedConcurrentHashMap
import com.amadeus.testfwk.{ConfigSupport, SimpleSpec}
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate

class SparklEarListenerSpec extends SimpleSpec with ConfigSupport {
  describe(s"The listener") {
    it("should instantiate LogSink") {
      val c = new SparkConf(false)
        .set("spark.sparklear.sink.class", "com.amadeus.sparklear.LogSink")
      new SparklEarListener(c)
    }

    it("should instantiate JsonSink") {
      val c = new SparkConf(false)
        .set("spark.sparklear.sink.class", "com.amadeus.sparklear.JsonSink")
      new SparklEarListener(c)
    }

    it("should throw IllegalArgumentException if spark.sparklear.sink.class not set") {
      an[IllegalArgumentException] should be thrownBy {
        new SparklEarListener(new SparkConf(false))
      }
    }

    it("should throw ClassNotFoundException if spark.sparklear.sink.class is invalid") {
      an[ClassNotFoundException] should be thrownBy {
        val c = new SparkConf(false)
          .set("spark.sparklear.sink.class", "com.amadeus.sparklear.DummySink")
        new SparklEarListener(c)
      }
    }
  }
}