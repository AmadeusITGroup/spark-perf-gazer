package com.amadeus.sparklear

import com.amadeus.airbi.SparkSpecification
import com.amadeus.testfwk.{ConfigSupport, JsonSupport, OptdSupport}

class SparklEarSpec extends SparkSpecification with OptdSupport with JsonSupport with ConfigSupport {

  "The listener" should "generate a basic SQL report" in withSpark() { spark =>
    val df = readOptd(spark)
    val eventsListener = new SparklEar(defaultTestConfig)
    spark.sparkContext.addSparkListener(eventsListener)
    df.write.format("noop").mode("overwrite").save()
    spark.sparkContext.removeSparkListener(eventsListener)
    val reports = eventsListener.stringReports
    reports.size shouldBe 1
    val report0 = reports.head
    query(report0, ".id") shouldEqual Array(1)
    query(report0, ".p.nodeName") shouldEqual Array("OverwriteByExpression")
    query(report0, ".p.children[0].nodeName") shouldEqual Array("Scan csv ")
    query(report0, ".p.children[0].metrics[*].name") shouldEqual
      Array("number of output rows", "number of files read", "metadata time", "size of files read")
    query(report0, ".p.children[0].metrics[*].metricType") shouldEqual
      Array("KO", "OK", "OK", "OK")
    query(report0, ".p.children[0].metrics[*].accumulatorId") shouldEqual
      Array(-1, 1, 0, 44662949)
  }

}
