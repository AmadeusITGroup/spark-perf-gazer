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

          When("a partitioned lookup table is created and joined with the main table")
          // Create a lookup table partitioned by country_code, containing 252 rows
          spark.sparkContext.setJobDescription("joblookuptable")
          df
            .select("country_code", "country_name")
            .distinct()
            .write
            .format("delta")
            .partitionBy("country_code")
            .mode("overwrite")
            .save(subdir(tmpDir, "joblookuptabledir"))

          // JOIN with the lookup table, filtering on a partition column (country_code)
          // and a data column (iata_code). This produces both PartitionFilters and PushedFilters.
          spark.sparkContext.setJobDescription("jobjoin")
          val df3 = df
            .select("name", "iata_code", "country_code")
            .filter(df("iata_code") === "COR")
            .filter(df("country_code") === "AR")
            .as("l")
            .join(
              DeltaTable.forPath(subdir(tmpDir, "joblookuptabledir")).toDF.as("r"),
              "country_code"
            )
          df3.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadirjob3"))

          Thread.sleep(3000)
          spark.sparkContext.removeSparkListener(eventsListener)
          eventsListener.close()

          Then("the SQL reports should be queryable via the standard PerfGazer views")
          val snippets = eventsListener.getSnippets
          snippets.foreach(spark.sql)

          // The join query reads from two Parquet tables: the main OPTD table (filtered by
          // iata_code = "COR" and country_code = "AR") and the partitioned lookup table.
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

          // Use AnalysisQueries.FiltersPerScan to extract all filter predicates from the SQL
          // plan details. The query returns one row per SQL execution with arrays of all
          // locations, partitionFilters, pushedFilters, and dataFilters found in the plan.
          And("it should extract filter predicates for the jobjoin query")
          val filterResults = spark.sql(AnalysisQueries.FiltersPerScan).collect()
          val joinRow = filterResults.find(_.getAs[String]("description") == "jobjoin").get

          // The main table scan: iata_code and country_code filters are pushed down.
          // No partition filters since the main table is not partitioned.
          val pushedFilters = joinRow.getAs[Seq[String]]("pushedFilters")
          pushedFilters should contain(
            "IsNotNull(iata_code), IsNotNull(country_code), EqualTo(iata_code,COR), EqualTo(country_code,AR)"
          )

          // The lookup table scan: country_code = "AR" is a partition filter (the table is
          // partitioned by country_code), so it appears in partitionFilters, not pushedFilters.
          val partitionFilters = joinRow.getAs[Seq[String]]("partitionFilters")
          partitionFilters.exists(_.contains("country_code")) should be(true)

          // Each scan should report a location pointing to the table directory.
          val locations = joinRow.getAs[Seq[String]]("locations")
          locations should not be empty
          locations.foreach(_ should include("file:"))

          // The lookup table creation (select distinct — no filter) should have no filters.
          And("it should report no filters for the joblookuptable query")
          val lookupRow = filterResults.find(_.getAs[String]("description") == "joblookuptable").get
          lookupRow.getAs[Seq[String]]("partitionFilters").filter(_.nonEmpty) should be(empty)
          lookupRow.getAs[Seq[String]]("pushedFilters").filter(_.nonEmpty) should be(empty)

          // With small datasets, no spill should occur.
          And("it should report no jobs with spill")
          // Testing spill is difficult, for now we only check that the query syntax is correct
          val spillResults = spark.sql(AnalysisQueries.JobsWithSpill).collectAs("jobName")
          spillResults should be(empty)
        }
      }
    }
  }
}
