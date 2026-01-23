package com.amadeus.integration

import com.amadeus.perfgazer.PerfGazer
import com.amadeus.perfgazer.reports.SqlReport
import com.amadeus.testfwk._
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.filters.SqlNodeFilter
import io.delta.tables.DeltaTable

import java.nio.file.Path

class JoinFromDeltaSpec
    extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with ConfigSupport
    with TempDirSupport {
  val DeltaSettings: List[(String, String)] = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false")
  )

  private def subdir(base: Path, s: String) = base.resolve(s).toAbsolutePath.toFile.toString

  describe("The listener when joining two dataframes read from delta") {
    it("should report scan and join nodes for the delta join") {
      withSpark(DeltaSettings, appName = this.getClass.getName) { spark =>
        withTmpDir { tmpDir =>
          val df = readOptd(spark)
          df.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadir1"))

          val sinks = new TestableSink()
          // DF TABLE: iata_code, icao_code, ..., name, ..., country_name, country_code, ...
          val df = DeltaTable.forPath(subdir(tmpDir, "deltadir1")).toDF
          val cfg = defaultTestConfig.withOnlySqlEnabled

          val eventsListener = new PerfGazer(cfg, sinks)
          spark.sparkContext.addSparkListener(eventsListener)

          // Create a lookup table with country_code and country_name, containing 252 rows
          spark.sparkContext.setJobDescription("joblookuptable")
          df
            .select("country_code", "country_name")
            .distinct()
            .coalesce(1) // to have only one file
            .write
            .format("delta")
            .mode("overwrite")
            .save(subdir(tmpDir, "joblookuptabledir"))

          // JOIN with the lookup table
          spark.sparkContext.setJobDescription("jobjoin")
          val df3 = df
            .select("name", "country_code")
            .filter(df("iata_code") === "COR")
            .as("l")
            .join(DeltaTable.forPath(subdir(tmpDir, "joblookuptabledir")).toDF.as("r"), "country_code")
          df3.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadirjob3"))

          val scanFilter = SqlNodeFilter(
            nodeNameRegex = Some(".*Scan parquet.*"),
            jobNameRegex = Some("jobjoin"),
            isLeaf = Some(true)
          )
          val scanActual = sinks.reports
            .collect { case i: SqlReport => i.nodes }
            .flatten
            .filter(r => scanFilter.eligible(r))
            .map(i => (i.jobName, i.metrics("number of files read"), i.metrics("number of output rows")))
          scanActual.size should equal(2) // one for the build side and one for the probe side
          scanActual should contain(("jobjoin", "1", "252")) // lookup table scan (probe side)

          val joinFilter = SqlNodeFilter(
            nodeNameRegex = Some(".*Join.*"),
            jobNameRegex = Some("jobjoin")
          )
          val joinActual = sinks.reports
            .collect { case i: SqlReport => i.nodes }
            .flatten
            .filter(r => joinFilter.eligible(r))
            .map(i => (i.jobName, i.metrics))
          joinActual should equal(Seq(("jobjoin", Map("number of output rows" -> "2"))))
        }
      }
    }
  }
}
