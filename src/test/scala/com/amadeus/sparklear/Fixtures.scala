package com.amadeus.sparklear

import com.amadeus.sparklear.prereports.SqlPreReport
import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import com.amadeus.sparklear.collects.SqlCollect

object Fixtures {

  object SqlWrapper1 {

    val p2 = new org.apache.spark.sql.execution.SparkPlanInfo(
      nodeName = "Scan csv ",
      simpleString = "FileScan csv [iata_code#17,icao_code#18,faa_code#19,is_geonames#20,geoname_id#21,envelope_id#22,name#23,asciiname#24,latitude#25,longitude#26,fclass#27,fcode#28,page_rank#29,date_from#30,date_until#31,comment#32,country_code#33,cc2#34,country_name#35,continent_name#36,adm1_code#37,adm1_name_utf#38,adm1_name_ascii#39,adm2_code#40,... 27 more fields] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex(1 paths)[file:/home/mjost/workspace/sparklear/src/test/resources/optd_por_publi..., PartitionFilters: [], PushedFilters: [], ReadSchema: struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,env...",
      children = Seq.empty[SparkPlanInfo],
      metadata = Map(
        "Location" -> "InMemoryFileIndex(1 paths)[file:/home/mjost/workspace/sparklear/src/test/resources/optd_por_public_all.csv])",
        "ReadSchema" -> "struct<iata_code:string,icao_code:string,faa_code:string,is_geonames:string,geoname_id:string,envelope_id:string,name:string,asciiname:string,latitude:string,longitude:string,fclass:string,fcode:string,page_rank:string,date_from:string,date_until:string,comment:string,country_code:string,cc2:string,country_name:string,continent_name:string,adm1_code:string,adm1_name_utf:string,adm1_name_ascii:string,adm2_code:string,adm2_name_utf:string,adm2_name_ascii:string,adm3_code:string,adm4_code:string,population:string,elevation:string,gtopo30:string,timezone:string,gmt_offset:string,dst_offset:string,raw_offset:string,moddate:string,city_code_list:string,city_name_list:string,city_detail_list:string,tvl_por_list:string,iso31662:string,location_type:string,wiki_link:string,alt_name_section:string,wac:string,wac_name:string,ccy_code:string,unlc_list:string,uic_list:string,geoname_lat:string,geoname_lon:string>)",
        "Format" -> "CSV",
        "Batched" -> "false",
        "PartitionFilters" -> "[]",
        "PushedFilters" -> "[]",
        "DataFilters" -> "[]"
      ),
      metrics = Seq(
        new SQLMetricInfo("number of output rows", 45, "sum"),
        new SQLMetricInfo("number of files read", 46, "sum"),
        new SQLMetricInfo("metadata time", 47, "timing"),
        new SQLMetricInfo("size of files read", 48, "size")
      )
    )

    val p1 = new org.apache.spark.sql.execution.SparkPlanInfo(
      nodeName = "OverwriteByExpression",
      simpleString = "OverwriteByExpression ...",
      children = Seq(p2),
      metadata = Map.empty[String, String],
      metrics = Seq.empty[SQLMetricInfo]
    )

    val rootSqlReport = SqlPreReport(
      collect = SqlCollect(
        id  = 1,
        plan = p1,
        description = "toto"
      ),
      metrics = Map(
        48L -> 44662949L,
        46L -> 1L,
        47L -> 0L
      )
    )
  }

}
