package com.amadeus.sparklear

import com.amadeus.sparklear.converters.{JobPretty, SqlJson, SqlJsonFlat, SqlPretty, SqlSingleLine, StagePretty}
import com.amadeus.sparklear.input.{JobInput, SqlInput, StageInput}
import com.amadeus.sparklear.output.glasses.SqlNodeGlass
import com.amadeus.sparklear.output.{OutputString, SqlNode}
import com.amadeus.testfwk.{ConfigSupport, JsonSupport, OptdSupport, SimpleSpec, SparkSupport}

class ReadCsvToNoopSpec extends SimpleSpec with SparkSupport with OptdSupport with JsonSupport with ConfigSupport {

  describe("The listener when reading a .csv and writing to noop") {
    withSpark(
      //List(("spark.ui.enabled", "true"))
    ) { spark =>
      val df = readOptd(spark)
      val cfg = defaultTestConfig.withAllEnabled
      val eventsListener = new SparklEar(cfg)
      spark.sparkContext.addSparkListener(eventsListener)
      spark.sparkContext.setJobGroup("test group", "test job")
      df.write.format("noop").mode("overwrite").save()
      spark.sparkContext.removeSparkListener(eventsListener)
      val inputs = eventsListener.inputs
      inputs.size shouldBe 3

      describe("should generate a basic SQL report") {
        // with JSON serializer
        val inputSql = inputs.collect { case s: SqlInput => s }.head
        it("with json serializer") {
          val r = SqlJson.toStringReport(cfg, inputSql)
          query(r, ".id") shouldEqual Array(1)
          query(r, ".p.nodeName") shouldEqual Array("OverwriteByExpression")
          query(r, ".p.children[0].nodeName") shouldEqual Array("Scan csv ")
          query(r, ".p.children[0].metrics[*].name") shouldEqual
            Array("number of output rows", "number of files read", "metadata time", "size of files read")
          query(r, ".p.children[0].metrics[*].metricType") shouldEqual Array("OK", "OK", "OK", "OK")
          query(r, ".p.children[0].metrics[*].accumulatorId") shouldEqual Array(
            124189,
            1,
            0,
            44662949
          ) // scalastyle:ignore magic.number
        }
        it("with jsonflat serializer") {
          val r = SqlJsonFlat.toOutput(cfg, inputSql)
          r.size should be(2)
          r.head should be(SqlNode(1, "OverwriteByExpression", 1, "0", Seq.empty[(String, String)]))
          val m = Seq(
            ("number of output rows", "124189"),
            ("number of files read", "1"),
            ("metadata time", "0"),
            ("size of files read", "44662949")
          )
          r.last should be(SqlNode(1, "Scan csv ", 2, "0.0", m))
        }
        describe("with jsonflat serializer filtered") {
          it ("by nodename") {
            val g = Seq(SqlNodeGlass(nodeNameRegex = Some(".*ByExpr.*")))
            val cfg = defaultTestConfig.withAllEnabled.withGlasses(g)
            val r = SqlJsonFlat.toOutput(cfg, inputSql)
            r should be(Seq(SqlNode(1, "OverwriteByExpression", 1, "0", Seq.empty[(String, String)])))
          }
          it ("by metric number_of_files_read") {
            val g = Seq(SqlNodeGlass(metricRegex = Some("number of files read")))
            val cfg = defaultTestConfig.withAllEnabled.withGlasses(g)
            val r = SqlJsonFlat.toOutput(cfg, inputSql)
            r.map(_.name) should be(Seq("Scan csv "))
          }
        }
        it("with pretty serializer") {
          val r = SqlPretty.toStringReport(cfg, inputSql)
          r should include regex ("Operator OverwriteByExpression | OverwriteByExpression")
          // scalastyle:off line.size.limit
          r should include regex ("  Operator Scan csv  | FileScan csv \\[iata_code#17,icao_code#18,faa_code#19,is_geonames#20,geoname_id#21,envelope_id#22,name#23,asciiname#24,latitude#25,longitude#26,fclass#27,fcode#28,page_rank#29,date_from#30,date_until#31,comment#32,country_code#33,cc2#34,country_name#35,continent_name#36,adm1_code#37,adm1_name_utf#38,adm1_name_ascii#39,adm2_code#40,... 27 more fields\\] Batched: false, DataFilters: \\[\\], Format: CSV, Location: InMemoryFileIndex\\(1 paths\\)\\[file:.*/src/test/resources/optd_por_publi..., PartitionFilters: \\[\\], PushedFilters: \\[\\], ReadSchema: struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,env...")
          r should include regex ("   - Metadata: Map\\(Location -> InMemoryFileIndex\\(1 paths\\)\\[file:/.*src/test/resources/optd_por_public_all.csv\\], ReadSchema -> struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,envelope_id:string,name:string,asciiname:string,latitude:string,longitude:string,fclass:string,fcode:string,page_rank:string,date_from:string,date_until:string,comment:string,country_code:string,cc2:string,country_name:string,continent_name:string,adm1_code:string,adm1_name_utf:string,adm1_name_ascii:string,adm2_code:string,adm2_name_utf:string,adm2_name_ascii:string,adm3_code:string,adm4_code:string,population:string,elevation:string,gtopo30:string,timezone:string,gmt_offset:string,dst_offset:string,raw_offset:string,moddate:string,city_code_list:string,city_name_list:string,city_detail_list:string,tvl_por_list:string,iso31662:string,location_type:string,wiki_link:string,alt_name_section:string,wac:string,wac_name:string,ccy_code:string,unlc_list:string,uic_list:string,geoname_lat:string,geoname_lon:string>, Format -> CSV, Batched -> false, PartitionFilters -> \\[\\], PushedFilters -> \\[\\], DataFilters -> \\[\\]\\)")
          r should include regex ("   - Metrics: 'number_of_output_rows'=124189,'number_of_files_read'=1,'metadata_time'=.*,'size_of_files_read'=44662949")
          // scalastyle:on line.size.limit
        }

        it("with singleline serializer") {
          val r = SqlSingleLine.toOutput(cfg, inputSql)
          r should equal(Seq(OutputString("SQL_ID=1")))
        }
      }

      it("should generate a basic JOB report (pretty)") {
        // with PRETTY serializer
        val inputJob = inputs.collect { case s: JobInput => s }.head
        val r = JobPretty.toStringReport(cfg, inputJob)
        r should include regex ("JOB ID=1 GROUP='test group' NAME='test job' SQL_ID=1  STAGES=1 TOTAL_CPU_SEC=.*")
      }

      it("should generate a basic STAGE report (pretty)") {
        // with PRETTY serializer
        val inputStage = inputs.collect { case s: StageInput => s }.head
        val r = StagePretty.toStringReport(cfg, inputStage)
        r should include regex ("STAGE ID=1 READ_MB=42 WRITE_MB=0 SHUFFLE_READ_MB=0 SHUFFLE_WRITE_MB=0 EXEC_CPU_SECS=.* ATTEMPT=0")
      }

    }
  }

}
