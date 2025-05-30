package com.amadeus.sparklear.reports

import com.amadeus.sparklear.translators.Translator.EntityName

/**
  * The end-user information sharing unit
  *
  * It can represent one entity: an SQL query, a job, a stage, or even go at a deper level and represent one
  * of their constituent parts (like an SQL query node)
  */
trait Report {
  def entity: EntityName
  // def dataframe: ... TODO
  //def asJson: String = asJson(this)(DefaultFormats) // TODO: use a more efficient serialization
}
