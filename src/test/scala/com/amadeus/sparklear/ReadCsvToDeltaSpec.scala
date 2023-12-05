package com.amadeus.sparklear

import com.amadeus.sparklear.translators._
import com.amadeus.sparklear.prereports.{PreReport, SqlPreReport}
import com.amadeus.sparklear.reports.glasses.SqlNodeGlass
import com.amadeus.testfwk._
import io.delta.tables.DeltaTable

import java.nio.file.Path
import scala.collection.mutable.ListBuffer

class ReadCsvToDeltaSpec
    extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with TempDirSupport
    with SinkSupport {
  val DeltaSettings = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false")
  )

  def subdir(base: Path, s: String) = base.resolve(s).toAbsolutePath.toFile.toString

  describe("The listener when") {
    withSpark(DeltaSettings) { spark =>
      withTmpDir { tmpDir =>
        describe("reading a .csv and writing to delta") {
          val df = readOptd(spark)
          val prereports = new ListBuffer[PreReport]()
          val cfg = defaultTestConfig.withAllEnabled.withPreReportSink(prereports.+=)
          val eventsListener = new SparklEar(cfg)
          spark.sparkContext.addSparkListener(eventsListener)
          spark.sparkContext.setJobGroup("testgroup", "testjob1")
          df.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadir1"))
          spark.sparkContext.removeSparkListener(eventsListener)
          describe("should generate a basic SQL report") {
            // with JSON serializer
            val inputSqls = prereports.collect { case s: SqlPreReport => s }
            describe("with sqlnode translator filtered") {
              it("by nodename") {
                val g = Seq(SqlNodeGlass(nodeNameRegex = Some(".*Scan .*")))
                val cfg = defaultTestConfig.withAllEnabled.withGlasses(g)
                val r = inputSqls.flatMap(i => SqlPlanNodeTranslator.toAllReports(cfg, i))
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
}
