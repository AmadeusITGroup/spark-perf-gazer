package com.amadeus.integration

import com.amadeus.perfgazer.PerfGazer
import com.amadeus.perfgazer.reports.SqlReport
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.filters.SqlNodeFilter
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import io.delta.tables.DeltaTable

import java.nio.file.Path

class ReadCsvToDeltaSpec
    extends SimpleSpec {
  val DeltaSettings: List[(String, String)] = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false")
  )

  private def subdir(base: Path, s: String) = base.resolve(s).toAbsolutePath.toFile.toString

  describe("The listener when reading a delta table filtering and writing to another delta") {
    it("should report the filter plan nodes") {
      withSpark(DeltaSettings, appName = this.getClass.getName) { spark =>
        withTmpDir { tmpDir =>
          val optdDf = OptdSupport.readOptd(spark)
          optdDf.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadir1"))

          val sinks = new TestableSink()
          // DF TABLE: iata_code, icao_code, ..., name, ..., country_name, country_code, ...
          val df = DeltaTable.forPath(subdir(tmpDir, "deltadir1")).toDF
          val cfg = defaultTestConfig.withOnlySqlEnabled

          val eventsListener = new PerfGazer(cfg, sinks)
          spark.sparkContext.addSparkListener(eventsListener)

          // FILTER on adm1_name_ascii and fcode
          spark.sparkContext.setJobDescription("jobfilter")
          val dfFiltered1 =
            df.filter(df("adm1_name_ascii") === "Cordoba" && df("fcode") === "AIRP") // 16 records matching
          dfFiltered1.write.format("delta").mode("overwrite").save(subdir(tmpDir, "jobfilterdir"))

          val f = SqlNodeFilter(
            nodeNameRegex = Some(".*Filter.*"),
            parentNodeNameRegex = Some(".*WholeStageCodegen.*"),
            jobNameRegex = Some("jobfilter")
          )
          val actual = sinks.reports
            .collect { case i: SqlReport => i.nodes }
            .flatten
            .filter(r => f.eligible(r))
            .map(i => (i.jobName, i.metrics))
          actual should equal(Seq(("jobfilter", Map("number of output rows" -> "16"))))
        }
      }
    }
  }

}
