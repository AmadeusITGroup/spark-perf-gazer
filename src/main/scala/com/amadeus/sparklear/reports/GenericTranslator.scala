package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.Entity
import org.apache.avro.Schema
import org.apache.avro.generic.GenericRecord

/**
  * Converts a [[Report]] into a [[GenericRecord]]
  *
  * @tparam R the [[Report]] type
  * @tparam GR the [[GenericRecord]] type
  */
trait GenericTranslator[R <: Report, GR <: GenericRecord] {

  /**
    * Converts a [[Report]] R into a [[GenericRecord]] GR
    *
    * @param r the [[Report]] to convert
    * @return the [[GenericRecord]] generated
    */
  val reportSchema: Schema
  def fromReportToGenericRecord(r: R): GR
}
