package com.amadeus.sparklear

import com.amadeus.sparklear.translators.{
  JobPrettyTranslator,
  SqlPlanNodeTranslator,
  SqlPrettyTranslator,
  StagePrettyTranslator
}
import com.amadeus.sparklear.reports.glasses.SqlNodeGlass
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
      withAllSinks { sinks =>
        val df = readOptd(spark)
        val cfg = defaultTestConfig.withAllEnabled.withSinks(sinks)
        val eventsListener = new SparklEar(cfg)
        spark.sparkContext.addSparkListener(eventsListener)
        spark.sparkContext.setJobGroup("testgroup", "testjob")
        df.write.format("noop").mode("overwrite").save()

        it("should build some preReports") {
          sinks.preReports.size shouldBe 3
        }

        it("should build SQL preReports (SqlPlanNodeTranslator)") {
          val inputSql = sinks.sqlPreReports.head
          val r = SqlPlanNodeTranslator.toAllReports(cfg, inputSql)
          r.size should be(2)
          r.map(i => (i.sqlId, i.jobName, i.nodeName)).head should be(1, "testjob", "OverwriteByExpression")
          r.map(i => (i.sqlId, i.jobName, i.nodeName)).last should be(1, "testjob", "Scan csv ")
        }

        it("should build SQL preReports (SqlPlanNodeTranslator filtered by nodeName)") {
          val inputSql = sinks.sqlPreReports.head
          val g = Seq(SqlNodeGlass(nodeNameRegex = Some(".*ByExpr.*")))
          val cfg = defaultTestConfig.withAllEnabled.withGlasses(g)
          val r = SqlPlanNodeTranslator.toReports(cfg, inputSql)
          r.map(i => (i.sqlId, i.jobName, i.nodeName)) should be(Seq((1, "testjob", "OverwriteByExpression")))
        }
        it("should build SQL preReports (SqlPlanNodeTranslator filtered by metric)") {
          val inputSql = sinks.sqlPreReports.head
          val g = Seq(SqlNodeGlass(metricRegex = Some("number of files read")))
          val cfg = defaultTestConfig.withAllEnabled.withGlasses(g)
          val r = SqlPlanNodeTranslator.toReports(cfg, inputSql)
          r.map(_.nodeName) should be(Seq("Scan csv "))
        }
        it("should build SQL preReports (SqlPrettyTranslator filtered by metric)") {
          val inputSql = sinks.sqlPreReports.head
          val r = SqlPrettyTranslator.toStringReports(cfg, inputSql).mkString("\n")
          r should include regex ("Operator OverwriteByExpression | OverwriteByExpression")
          // scalastyle:off line.size.limit
          r should include regex ("  Operator Scan csv  | FileScan csv \\[iata_code#17,icao_code#18,faa_code#19,is_geonames#20,geoname_id#21,envelope_id#22,name#23,asciiname#24,latitude#25,longitude#26,fclass#27,fcode#28,page_rank#29,date_from#30,date_until#31,comment#32,country_code#33,cc2#34,country_name#35,continent_name#36,adm1_code#37,adm1_name_utf#38,adm1_name_ascii#39,adm2_code#40,... 27 more fields\\] Batched: false, DataFilters: \\[\\], Format: CSV, Location: InMemoryFileIndex\\(1 paths\\)\\[file:.*/src/test/resources/optd_por_publi..., PartitionFilters: \\[\\], PushedFilters: \\[\\], ReadSchema: struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,env...")
          r should include regex ("   - Metadata: Map\\(Location -> InMemoryFileIndex\\(1 paths\\)\\[file:/.*src/test/resources/optd_por_public_all.csv\\], ReadSchema -> struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,envelope_id:string,name:string,asciiname:string,latitude:string,longitude:string,fclass:string,fcode:string,page_rank:string,date_from:string,date_until:string,comment:string,country_code:string,cc2:string,country_name:string,continent_name:string,adm1_code:string,adm1_name_utf:string,adm1_name_ascii:string,adm2_code:string,adm2_name_utf:string,adm2_name_ascii:string,adm3_code:string,adm4_code:string,population:string,elevation:string,gtopo30:string,timezone:string,gmt_offset:string,dst_offset:string,raw_offset:string,moddate:string,city_code_list:string,city_name_list:string,city_detail_list:string,tvl_por_list:string,iso31662:string,location_type:string,wiki_link:string,alt_name_section:string,wac:string,wac_name:string,ccy_code:string,unlc_list:string,uic_list:string,geoname_lat:string,geoname_lon:string>, Format -> CSV, Batched -> false, PartitionFilters -> \\[\\], PushedFilters -> \\[\\], DataFilters -> \\[\\]\\)")
          r should include regex ("   - Metrics: 'number_of_output_rows'=124189,'number_of_files_read'=1,'metadata_time'=.*,'size_of_files_read'=44662949")
          // scalastyle:on line.size.limit
        }

        it("should build job preReports (JobPrettyTranslator)") {
          val inputJob = sinks.jobPreReports.head
          val r = JobPrettyTranslator.toStringReports(cfg, inputJob).mkString("\n")
          r should include regex ("JOB ID=1 GROUP='testgroup' NAME='testjob' SQL_ID=1  STAGES=1 TOTAL_CPU_SEC=.*")
        }

        it("should build stage preReports (StagePrettyTranslator)") {
          val inputStage = sinks.stagePreReports.head
          val r = StagePrettyTranslator.toStringReports(cfg, inputStage).mkString("\n")
          r should include regex ("STAGE ID=1 READ_MB=42 WRITE_MB=0 SHUFFLE_READ_MB=0 SHUFFLE_WRITE_MB=0 EXEC_CPU_SECS=.* ATTEMPT=0")
        }

      }
    }
  }

}
