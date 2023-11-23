package com.amadeus.sparklear

import com.amadeus.airbi.SparkSpecification
import com.amadeus.sparklear.reports.SqlReport
import com.amadeus.sparklear.reports.converters.{SqlJson, SqlPretty}
import com.amadeus.testfwk.{ConfigSupport, JsonSupport, OptdSupport}

class SparklEarSpec extends SparkSpecification with OptdSupport with JsonSupport with ConfigSupport {

  "The listener" should "generate a basic SQL report" in withSpark() { spark =>
    val df = readOptd(spark)
    val cfg = defaultTestConfig
    val eventsListener = new SparklEar(cfg)
    spark.sparkContext.addSparkListener(eventsListener)
    df.write.format("noop").mode("overwrite").save()
    val reports = eventsListener.reports
    spark.sparkContext.removeSparkListener(eventsListener)
    reports.size shouldBe 3

    val reportSql = reports.collect{case s: SqlReport => s}.head
    val rSqlJson = reportSql.toStringReport(cfg.withSqlSerializer(SqlJson))
    query(rSqlJson, ".id") shouldEqual Array(1)
    query(rSqlJson, ".p.nodeName") shouldEqual Array("OverwriteByExpression")
    query(rSqlJson, ".p.children[0].nodeName") shouldEqual Array("Scan csv ")
    query(rSqlJson, ".p.children[0].metrics[*].name") shouldEqual
      Array("number of output rows", "number of files read", "metadata time", "size of files read")
    query(rSqlJson, ".p.children[0].metrics[*].metricType") shouldEqual Array("KO", "OK", "OK", "OK")
    query(rSqlJson, ".p.children[0].metrics[*].accumulatorId") shouldEqual Array(-1, 1, 0, 44662949)

    val rSqlPretty = reportSql.toStringReport(cfg.withSqlSerializer(SqlPretty))
    rSqlPretty should include("Operator OverwriteByExpression | OverwriteByExpression")
    rSqlPretty should include("Operator Scan csv  | FileScan csv [iata_code#17,icao_code#18,faa_code#19,is_geonames#20,geoname_id#21,envelope_id#22,name#23,asciiname#24,latitude#25,longitude#26,fclass#27,fcode#28,page_rank#29,date_from#30,date_until#31,comment#32,country_code#33,cc2#34,country_name#35,continent_name#36,adm1_code#37,adm1_name_utf#38,adm1_name_ascii#39,adm2_code#40,... 27 more fields] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mjost/workspace/sparklear/src/test/resources/optd_por_publi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,env...")
    rSqlPretty should include(" - Metadata: Map(Location -> InMemoryFileIndex(1 paths)[file:/")
    rSqlPretty should include("src/test/resources/optd_por_public_all.csv], ReadSchema -> struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,envelope_id:string,name:string,asciiname:string,latitude:string,longitude:string,fclass:string,fcode:string,page_rank:string,date_from:string,date_until:string,comment:string,country_code:string,cc2:string,country_name:string,continent_name:string,adm1_code:string,adm1_name_utf:string,adm1_name_ascii:string,adm2_code:string,adm2_name_utf:string,adm2_name_ascii:string,adm3_code:string,adm4_code:string,population:string,elevation:string,gtopo30:string,timezone:string,gmt_offset:string,dst_offset:string,raw_offset:string,moddate:string,city_code_list:string,city_name_list:string,city_detail_list:string,tvl_por_list:string,iso31662:string,location_type:string,wiki_link:string,alt_name_section:string,wac:string,wac_name:string,ccy_code:string,unlc_list:string,uic_list:string,geoname_lat:string,geoname_lon:string>, Format -> CSV, Batched -> false, PartitionFilters -> [], PushedFilters -> [], DataFilters -> [])")
    rSqlPretty should include(" - Metrics: 'number_of_output_rows'=None(type=sum,id=45),'number_of_files_read'=Some(1)(type=sum,id=46),'metadata_time'=Some(0)(type=timing,id=47),'size_of_files_read'=Some(44662949)(type=size,id=48)")

  }

}
