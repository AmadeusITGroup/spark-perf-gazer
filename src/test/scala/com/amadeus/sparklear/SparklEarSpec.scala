package com.amadeus.sparklear

import com.amadeus.airbi.SparkSpecification
import com.amadeus.sparklear.reports.SqlReport
import com.amadeus.testfwk.{ConfigSupport, JsonSupport, OptdSupport}

class SparklEarSpec extends SparkSpecification with OptdSupport with JsonSupport with ConfigSupport {

  "The listener" should "generate a basic SQL report" in withSpark() { spark =>
    val df = readOptd(spark)
    val cfg = defaultTestConfig
    val eventsListener = new SparklEar(cfg)
    spark.sparkContext.addSparkListener(eventsListener)
    df.write.format("noop").mode("overwrite").save()
    val reports = eventsListener.reports
    spark.sparkContext.removeSparkListener(eventsListener)
    reports.size shouldBe 3
    val reportSql = reports.collect{case s: SqlReport => s}.head
    val report0 = reportSql.toStringReport(cfg)
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
