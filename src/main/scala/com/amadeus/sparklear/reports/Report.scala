package com.amadeus.sparklear.reports

/**
  * The end-user information sharing unit
  *
  * It can represent one entity: an SQL query, a job, a stage, or even go at a deper level and represent one
  * of their constituent parts (like an SQL query node)
  */
trait Report {
  type Json = String
  // def dataframe: ... TODO
  def asJson: Json
}
