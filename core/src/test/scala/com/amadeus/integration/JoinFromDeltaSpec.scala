package com.amadeus.integration

import com.amadeus.perfgazer.{AnalysisQueries, JsonSink, PerfGazer}
import com.amadeus.testfwk.ConfigSupport._
import com.amadeus.testfwk.DataFrameSupport._
import com.amadeus.testfwk.{OptdSupport, SimpleSpec}
import com.amadeus.testfwk.SparkSupport.withSpark
import com.amadeus.testfwk.TempDirSupport.withTmpDir
import io.delta.tables.DeltaTable
import org.apache.spark.SparkConf
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

          val destination = subdir(tmpDir, "perfgazer-output")
          val sparkConf = new SparkConf(false)
            .set(JsonSink.DestinationKey, destination)
          val jsonSink = new JsonSink(sparkConf)

          val df = DeltaTable.forPath(subdir(tmpDir, "deltadir1")).toDF
          val cfg = defaultTestConfig.withAllEnabled

          val eventsListener = new PerfGazer(cfg, jsonSink)
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

          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          eventsListener.close()

          Then("the SQL reports should be queryable via the standard PerfGazer views")
          val snippets = eventsListener.getSnippets
          snippets.foreach(spark.sql)

          // The join query reads from two Parquet tables: the main OPTD table (filtered by
          // iata_code = "COR") and the lookup table (country_code, country_name).
          // Each produces a Scan parquet leaf node in the physical plan.
          And("it should report the two scan parquet nodes: build side and probe side of the join")
          val scanNodesDf = spark.sql(
            """SELECT n.jobName, n.metrics.`number of files read` AS filesRead,
              |       n.metrics.`number of output rows` AS outputRows
              |  FROM sql sq
              |       LATERAL VIEW EXPLODE(sq.nodes) AS n
              | WHERE n.nodeName LIKE '%Scan parquet%'
              |   AND n.isLeaf = true
              |   AND n.jobName = 'jobjoin'""".stripMargin
          )
          val scanNodes = scanNodesDf.collectAs("jobName", "filesRead", "outputRows")
          scanNodes.length should equal(2)
          scanNodes should contain(
            ("jobjoin", "1", "252") // lookup table: 1 file, 252 distinct country rows
          )

          // The join between the filtered main table (2 rows matching COR) and the lookup
          // table should produce exactly 2 output rows.
          And("it should report the join plan node")
          val joinNodes = spark.sql(
            """SELECT n.jobName, n.metrics.`number of output rows` AS outputRows
              |  FROM sql sq
              |       LATERAL VIEW EXPLODE(sq.nodes) AS n
              | WHERE n.nodeName LIKE '%Join%'
              |   AND n.jobName = 'jobjoin'""".stripMargin
          ).collectAs("jobName", "outputRows")
          joinNodes should equal(Array(("jobjoin", "2")))

          // Use AnalysisQueries.PushedFiltersPerScan to extract all PushedFilters blocks
          // from the SQL plan details. The query returns one row per PushedFilters occurrence
          // across all SQL executions that contain Scan parquet leaf nodes.
          // The description column identifies which job the SQL execution belongs to.
          And("it should extract pushdown predicates on iata_code for the jobjoin query")
          val allResults = spark.sql(AnalysisQueries.PushedFiltersPerScan)
            .collectAs("description", "pushedFilters")

          // The filter df("iata_code") === "COR" should be pushed down to the Parquet reader
          // as IsNotNull + EqualTo predicates on the iata_code column.
          allResults should contain(
            ("jobjoin", "IsNotNull(iata_code), EqualTo(iata_code,COR), IsNotNull(country_code)")
          )

          // The lookup table creation (select distinct country_code, country_name — no filter)
          // should have no pushed predicates on any data column.
          And("it should report no pushdown predicates for the joblookuptable query")
          allResults should contain(("joblookuptable", ""))

          // With small datasets, no spill should occur.
          And("it should report no jobs with spill")
          val spillResults = spark.sql(AnalysisQueries.JobsWithSpill).collectAs("jobName")
          spillResults should be(empty)
        }
      }
    }
  }
}
