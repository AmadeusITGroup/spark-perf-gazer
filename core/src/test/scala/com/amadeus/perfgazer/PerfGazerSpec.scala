package com.amadeus.perfgazer

import com.amadeus.perfgazer.fixtures.Fixtures
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.SimpleSpec
import com.amadeus.testfwk.SinkSupport.TestableSink
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.ui.{SparkListenerSQLAdaptiveExecutionUpdate, SparkListenerSQLExecutionEnd}
import org.apache.spark.scheduler.{JobSucceeded, SparkListenerJobEnd}

import java.util.concurrent.atomic.AtomicInteger

class PerfGazerSpec extends SimpleSpec {
  describe(s"The listener") {
    it("should not fail upon unhandled messages") {
      val c = defaultTestConfig
      val l = new PerfGazer(c, new LogSink())
      val e = SparkListenerSQLAdaptiveExecutionUpdate(1, "", Fixtures.SqlWrapper1.planInfo1)
      l.onOtherEvent(e) // no failure
    }
    it("should log warning when job start event not found") {
      val c = defaultTestConfig
      val l = new PerfGazer(c, new LogSink())
      val e = SparkListenerJobEnd(
        jobId = 42,
        time = System.currentTimeMillis(),
        jobResult = JobSucceeded
      )
      l.onJobEnd(e)
    }
    it("should log warning when SQL start event not found") {
      val c = defaultTestConfig
      val l = new PerfGazer(c, new LogSink())
      val e = SparkListenerSQLExecutionEnd(
        executionId = 12345L,
        time = System.currentTimeMillis()
      )
      l.onOtherEvent(e)
    }

    it("should instantiate LogSink") {
      val c = new SparkConf(false)
        .set("spark.perfgazer.sink.class", "com.amadeus.perfgazer.LogSink")
      new PerfGazer(c)
    }

    it("should instantiate JsonSink") {
      val c = new SparkConf(false)
        .set("spark.perfgazer.sink.class", "com.amadeus.perfgazer.JsonSink")
        .set("spark.perfgazer.sink.json.destination", "/tmp/")
      new PerfGazer(c)
    }

    it("should throw IllegalArgumentException if spark.perfgazer.sink.class not set") {
      an[IllegalArgumentException] should be thrownBy {
        new PerfGazer(new SparkConf(false))
      }
    }

    it("should throw ClassNotFoundException if spark.perfgazer.sink.class is invalid") {
      an[ClassNotFoundException] should be thrownBy {
        val c = new SparkConf(false)
          .set("spark.perfgazer.sink.class", "com.amadeus.perfgazer.DummySink")
        new PerfGazer(c)
      }
    }

    it("should have an instance registered after creation") {
      val c = defaultTestConfig
      new PerfGazer(c, new LogSink())
      PerfGazer.instance shouldBe defined
    }

    it("should only close the sink once when close() is called multiple times") {
      val closeCount = new AtomicInteger(0)
      val sink = new TestableSink() {
        override def close(): Unit = closeCount.incrementAndGet()
      }
      val l = new PerfGazer(defaultTestConfig, sink)

      l.close()
      l.close()
      l.close()

      closeCount.get() shouldBe 1
    }
  }
}
