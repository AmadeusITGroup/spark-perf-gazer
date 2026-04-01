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
import org.scalatest.GivenWhenThen

import java.nio.file.Path

class JoinFromDeltaSpec
    extends SimpleSpec with GivenWhenThen {
  val DeltaSettings: List[(String, String)] = List(
    ("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"),
    ("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog"),
    ("spark.sql.adaptive.enabled", "false"),
    ("spark.driver.host", "localhost")
  )

  private def subdir(base: Path, s: String) = base.resolve(s).toAbsolutePath.toFile.toString

  describe("The listener when joining two dataframes read from delta") {
    it("should report scan and join nodes for the delta join") {
      withSpark(DeltaSettings, appName = this.getClass.getName) { spark =>
        withTmpDir { tmpDir =>
          Given("a delta table created from OPTD CSV data")
          val optdDf = OptdSupport.readOptd(spark)
          optdDf.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadir1"))

          val sinks = new TestableSink()
          val df = DeltaTable.forPath(subdir(tmpDir, "deltadir1")).toDF
          val cfg = defaultTestConfig.withOnlySqlEnabled

          val eventsListener = new PerfGazer(cfg, sinks)
          spark.sparkContext.addSparkListener(eventsListener)

          When("a lookup table is created and joined with the main table")
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

          Then("it should report the two scan parquet nodes: build side and probe side of the join")
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

          And("it should report the join plan node")
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
