package com.amadeus.sparklear

import com.amadeus.testfwk.{ConfigSupport, OptdSupport, SinkSupport, SparkSupport}

object Sample extends SparkSupport with OptdSupport with ConfigSupport with SinkSupport {

  def main(s: Array[String]): Unit = {
    withSpark() { spark =>
      val df = readOptd(spark)
      val cfg = defaultTestConfig.withAllEnabled
      val parquetSinks = new ParquetSink(
        destination = "src/test/parquet-sink",
        writeBatchSize = 1,
        debug = true
      )
      val parquetEventsListener = new SparklEar(cfg.withAllEnabled.withSink(parquetSinks))
      spark.sparkContext.addSparkListener(parquetEventsListener)

      spark.sparkContext.setJobGroup("testgroup", "testjob")
      df.write.format("noop").mode("overwrite").save()
      Thread.sleep(1000 * 5) // wait for events cause callbacks asynchronously
      spark.sparkContext.removeSparkListener(parquetEventsListener)
      parquetSinks.flush()
      //Thread.sleep(1000 * 5) // not needed because above call is synchronous (and spark session will not be stopped until reaching the end of this scope)
      println(s"session is stopped: ${spark.sparkContext.isStopped}")
    }

  }
}
