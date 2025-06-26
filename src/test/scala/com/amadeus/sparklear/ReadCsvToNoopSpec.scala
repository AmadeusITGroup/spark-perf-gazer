package com.amadeus.sparklear

import com.amadeus.sparklear.reports.SqlReport
import com.amadeus.testfwk.SinkSupport.TestableSink
import com.amadeus.testfwk.{ConfigSupport, JsonSupport, OptdSupport, SimpleSpec, SinkSupport, SparkSupport}


class ReadCsvToNoopSpec
    extends SimpleSpec
    with SparkSupport
    with OptdSupport
    with JsonSupport
    with ConfigSupport
    with SinkSupport {

  describe("The listener when reading a .csv and writing to noop") {
    withSpark() { spark =>
      withTestableSink { sinks =>
        val df = readOptd(spark)

        // regular setup
        val cfg = defaultTestConfig.withAllEnabled.withSink(sinks)
        val eventsListener = new SparklEar(cfg)
        spark.sparkContext.addSparkListener(eventsListener)

        // setup to check if disabling all entities yields no reports in sinks
        val emptySinks = new TestableSink()
        val emptyEventsListener = new SparklEar(cfg.withAllDisabled.withSink(emptySinks))
        spark.sparkContext.addSparkListener(emptyEventsListener)

        // setup to write reports in json sink
        // val jsonSinks = new JsonSink(
        //   destination = "src/test/json-sink",
        //  writeBatchSize = 5,
        //  debug = true
        // )
        // val jsonEventsListener = new SparklEar(cfg.withAllEnabled.withSink(jsonSinks))
        // spark.sparkContext.addSparkListener(jsonEventsListener)

        // setup to write reports in json sink
        val parquetSinks = new ParquetSink(
          spark = spark,
          destination = "src/test/parquet-sink",
          writeBatchSize = 1,
          debug = true
        )
        val parquetEventsListener = new SparklEar(cfg.withAllEnabled.withSink(parquetSinks))
        spark.sparkContext.addSparkListener(parquetEventsListener)

        spark.sparkContext.setJobGroup("testgroup", "testjob")
        println(s"DEBUG : Spark Seesion used : ${spark.sparkContext}")
        // df.show()
        df.write.format("noop").mode("overwrite").save()

        it("should build some reports") {
          sinks.reports.size shouldBe 3
        }

        it("should build SQL preReports (SqlPlanNodeTranslator)") {
          val inputSql = sinks.reports.collect{ case r: SqlReport => r}.head
          val r = inputSql.nodes
          r.size should be(2)
          r.map(i => (i.sqlId, i.jobName, i.nodeName)).head should be(1, "testjob", "() OverwriteByExpression")
          r.map(i => (i.sqlId, i.jobName, i.nodeName)).last should be(1, "testjob", "() Scan csv ")
        }

        it("should build SQL preReports (SqlPlanNodeTranslator filtered by nodeName)") {
          //val inputSql = sinks.sqlPreReports.head
          //val g = Seq(SqlNodeFilter(nodeNameRegex = Some(".*ByExpr.*")))
          //val cfg = defaultTestConfig.withAllEnabled.withFilters(g)
          //val r = SqlPlanNodeTranslator.toReports(cfg, inputSql)
          //r.map(i => (i.sqlId, i.jobName, i.nodeName)) should be(Seq((1, "testjob", "OverwriteByExpression")))
        }
        it("should build SQL preReports (SqlPlanNodeTranslator filtered by metric)") {
          //val inputSql = sinks.sqlPreReports.head
          //val g = Seq(SqlNodeFilter(metricRegex = Some("number of files read")))
          //val cfg = defaultTestConfig.withAllEnabled.withFilters(g)
          //val r = SqlPlanNodeTranslator.toReports(cfg, inputSql)
          //r.map(_.nodeName) should be(Seq("Scan csv "))
        }
        it("should build SQL preReports (SqlPrettyTranslator filtered by metric)") {
          //val inputSql = sinks.sqlPreReports.head
          //val r = SqlPrettyTranslator.toStringReports(cfg, inputSql).mkString("\n")
          //r should include regex ("Operator OverwriteByExpression | OverwriteByExpression")
          // scalastyle:off line.size.limit
          //r should include regex ("  Operator Scan csv ")
          //r should include regex ("   - Metadata: Map\\(Location -> InMemoryFileIndex\\(1 paths\\)\\[file:/.*src/test/resources/optd_por_public_all.csv\\], ReadSchema -> struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,envelope_id:string,name:string,asciiname:string,latitude:string,longitude:string,fclass:string,fcode:string,page_rank:string,date_from:string,date_until:string,comment:string,country_code:string,cc2:string,country_name:string,continent_name:string,adm1_code:string,adm1_name_utf:string,adm1_name_ascii:string,adm2_code:string,adm2_name_utf:string,adm2_name_ascii:string,adm3_code:string,adm4_code:string,population:string,elevation:string,gtopo30:string,timezone:string,gmt_offset:string,dst_offset:string,raw_offset:string,moddate:string,city_code_list:string,city_name_list:string,city_detail_list:string,tvl_por_list:string,iso31662:string,location_type:string,wiki_link:string,alt_name_section:string,wac:string,wac_name:string,ccy_code:string,unlc_list:string,uic_list:string,geoname_lat:string,geoname_lon:string>, Format -> CSV, Batched -> false, PartitionFilters -> \\[\\], PushedFilters -> \\[\\], DataFilters -> \\[\\]\\)")
          //r should include regex ("   - Metrics: number of output rows -> 124189,number of files read -> 1,metadata time -> .*,size of files read -> 44662949")
          // scalastyle:on line.size.limit
        }

        it("should build job preReports (JobPrettyTranslator)") {
          //val inputJob = sinks.jobPreReports.head
          //val r = JobPrettyTranslator.toStringReports(cfg, inputJob).mkString("\n")
          //r should include regex ("JOB ID=1 GROUP='testgroup' NAME='testjob' SQL_ID=1 STAGES=1 TOTAL_CPU_SEC=.*")
        }

        it("should build stage preReports (StagePrettyTranslator)") {
          //val inputStage = sinks.stagePreReports.head
          //val r = StagePrettyTranslator.toStringReports(cfg, inputStage).mkString("\n")
          //r should include regex ("STAGE ID=1 READ_MB=42 WRITE_MB=0 SHUFFLE_READ_MB=0 SHUFFLE_WRITE_MB=0 EXEC_CPU_SECS=(\\d+) EXEC_RUN_SECS=(\\d+) EXEC_JVM_GC_SECS=(\\d+) ATTEMPT=0")
        }

        it("should not generate any report if all is disabled") {
          //emptySinks.entities.size should be(0)
          //emptySinks.reports.size should be(0)
          //emptySinks.stringReports.size should be(0)
        }

      }
    }
  }

}
