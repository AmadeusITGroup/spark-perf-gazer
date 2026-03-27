package com.amadeus.testfwk

import com.jayway.jsonpath.JsonPath
import net.minidev.json.JSONArray

object JsonSupport {
  type JsonString = String
  type JsonpathQuery = String
  def query(json: JsonString, query: JsonpathQuery): Array[AnyRef] =
    JsonPath.parse(json).read(query).asInstanceOf[JSONArray].toArray
}
