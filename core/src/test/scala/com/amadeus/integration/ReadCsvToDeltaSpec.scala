package com.amadeus.integration

import com.amadeus.sparklear.SparklEar
import com.amadeus.sparklear.reports.SqlReport
import com.amadeus.testfwk._
import com.amadeus.testfwk.filters.SqlNodeFilter
import io.delta.tables.DeltaTable

import java.nio.file.Path

class ReadCsvToDeltaSpec
    extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with TempDirSupport
    with SinkSupport {
  val DeltaSettings: List[(String, String)] = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false")
  )

  private def subdir(base: Path, s: String) = base.resolve(s).toAbsolutePath.toFile.toString

  describe("The listener when reading a delta table filtering and writing to another delta") {
    withSpark(DeltaSettings, appName = this.getClass.getName) { spark =>
      withTmpDir { tmpDir =>
        val df = readOptd(spark)
        df.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadir1"))

        withTestableSink { sinks =>
          // DF TABLE: iata_code, icao_code, ..., name, ..., country_name, country_code, ...
          val df = DeltaTable.forPath(subdir(tmpDir, "deltadir1")).toDF
          val cfg = defaultTestConfig.withOnlySqlEnabled.withSink(sinks)

          val eventsListener = new SparklEar(cfg)
          spark.sparkContext.addSparkListener(eventsListener)

          // FILTER on adm1_name_ascii and fcode
          spark.sparkContext.setJobDescription("jobfilter")
          val dfFiltered1 =
            df.filter(df("adm1_name_ascii") === "Cordoba" && df("fcode") === "AIRP") // 16 records matching
          dfFiltered1.write.format("delta").mode("overwrite").save(subdir(tmpDir, "jobfilterdir"))

          it("should report the filter plan nodes") {
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

}
