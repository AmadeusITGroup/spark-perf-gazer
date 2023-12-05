package com.amadeus.sparklear

import com.amadeus.sparklear.translators._
import com.amadeus.sparklear.prereports.{PreReport, SqlPreReport}
import com.amadeus.sparklear.reports.Report
import com.amadeus.sparklear.reports.glasses.SqlNodeGlass
import com.amadeus.testfwk._

import scala.collection.mutable.ListBuffer

class ReadCsvToDeltaSpec
    extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with TempDirSupport {
  val DeltaSettings = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false")
  )

  describe("The listener when reading a .csv and writing to delta") {
    withSpark(DeltaSettings) { spark =>
      withTmpDir { tmpDir =>
        val df = readOptd(spark)
        val inputs = new ListBuffer[PreReport]()
        val cfg = defaultTestConfig.withAllEnabled.withPreReportSink(inputs.+=)
        val eventsListener = new SparklEar(cfg)
        spark.sparkContext.addSparkListener(eventsListener)
        spark.sparkContext.setJobGroup("test group", "test job")
        df.write.format("delta").mode("overwrite").save(tmpDir.toAbsolutePath.toFile.toString)

        spark.sparkContext.removeSparkListener(eventsListener)

        describe("should generate a basic SQL report") {
          // with JSON serializer
          val inputSqls = inputs.collect { case s: SqlPreReport => s }
          describe("with jsonflat serializer filtered") {
            it("by nodename") {
              val g = Seq(SqlNodeGlass(nodeNameRegex = Some(".*Scan .*")))
              val cfg = defaultTestConfig.withAllEnabled.withGlasses(g)
              val r = inputSqls.flatMap(i => SqlNodeTranslator.toAllReports(cfg, i))
              r.map(i => i.nodeName).distinct should contain allOf (
                "Scan csv ",
                "Scan ExistingRDD",
                "Scan json "
              ) // we read the CSV, then scan json (delta log) and ExistingRDD (for writing to delta)
              r.filter(_.nodeName.matches("Scan ExistingRDD Delta Table State.*")).size should equal(1)
            }
          }
        }
      }
    }
  }

}
