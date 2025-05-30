package com.amadeus.sparklear

import com.amadeus.sparklear.reports.SqlPlanNodeReport
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
  val DeltaSettings = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false")
  )

  def subdir(base: Path, s: String) = base.resolve(s).toAbsolutePath.toFile.toString

  describe("The listener when reading a delta table filtering and writing to another delta") {
    withSpark(DeltaSettings) { spark =>
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
              .collect { case i: SqlPlanNodeReport => i }
              .filter(r => f.eligible(r))
              .map(i => (i.jobName, i.metrics))
            actual should equal(Seq(("jobfilter", Map("number of output rows" -> "16"))))
          }

          // LOOKUP TABLE: country_code, country name
          spark.sparkContext.setJobDescription("joblookuptable")
          df
            .select("country_code", "country_name")
            .distinct()
            .write
            .format("delta")
            .mode("overwrite")
            .save(subdir(tmpDir, "joblookuptabledir"))

          // TODO asserts for joblookuptable

          // JOIN with the lookup table
          spark.sparkContext.setJobDescription("jobjoin")
          val df3 = df
            .select("name", "country_code")
            .filter(df("iata_code") === "COR")
            .as("l")
            .join(DeltaTable.forPath(subdir(tmpDir, "joblookuptabledir")).toDF.as("r"), "country_code")
          df3.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadirjob3"))

          // TODO asserts for Scan node

          it("should report the join plan node") {
            val f = SqlNodeFilter(
              nodeNameRegex = Some(".*Join.*"),
              jobNameRegex = Some("jobjoin")
            )
            val actual = sinks.reports
              .collect { case i: SqlPlanNodeReport => i }
              .filter(r => f.eligible(r))
              .map(i => (i.jobName, i.metrics))
            actual should equal(Seq(("jobjoin", Map("number of output rows" -> "2"))))
          }
        }
      }
    }
  }
}
