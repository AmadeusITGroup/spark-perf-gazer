package com.amadeus.sparklear

import com.amadeus.sparklear.prereports.PreReport
import com.amadeus.sparklear.reports.SqlPlanNodeReport
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
        val df = readOptd(spark)
        df.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadir1"))

        describe("reading a delta table filtering and writing to another delta") {
          withAllSinks { sinks =>
            val df = DeltaTable.forPath(subdir(tmpDir, "deltadir1")).toDF
            val cfg = defaultTestConfig.withOnlySqlEnabled
              .withGlasses(
                Seq(
                  SqlNodeGlass(
                    nodeNameRegex = Some(".*Filter.*"),
                    parentNodeNameRegex = Some(".*WholeStageCodegen.*")
                  )
                )
              )
              .withSinks(sinks)
            val eventsListener = new SparklEar(cfg)
            spark.sparkContext.addSparkListener(eventsListener)

            spark.sparkContext.setJobDescription("job1")
            val dfFiltered1 = df.filter(df("iata_code") === "EZE") // 1 record matching
            dfFiltered1.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadirjob1"))

            spark.sparkContext.setJobDescription("job2")
            val dfFiltered2 =
              df.filter(df("adm1_name_ascii") === "Cordoba" && df("fcode") === "AIRP") // 16 records matching
            dfFiltered2.write.format("delta").mode("overwrite").save(subdir(tmpDir, "deltadirjob2"))

            spark.sparkContext.removeSparkListener(eventsListener)
            it("should report the filter plan node resulting in 1 record") {
              val actual = sinks.reports
                .collect { case i: SqlPlanNodeReport => i }
                .filter(_.jobName == "job1")
                .flatMap(_.metrics)
              actual should equal(Seq(("number of output rows", "1")))
            }
            it("should report the filter plan node resulting in 16 records") {
              val actual = sinks.reports
                .collect { case i: SqlPlanNodeReport => i }
                .filter(_.jobName == "job2")
                .flatMap(_.metrics)
              actual should equal(Seq(("number of output rows", "16")))
            }
          }
        }
      }
    }
  }
}
