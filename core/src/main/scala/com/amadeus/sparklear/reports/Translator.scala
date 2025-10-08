package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.Entity

/**
  * Converts a [[Entity]] into a [[Report]]
  *
  * @tparam E the [[Entity]] type
  * @tparam R the [[Report]] type
  */
trait Translator[E <: Entity, R <: Report] {

  /**
    * Convert a [[Entity]] P into a collection of [[Report]] R
    *
    * @param c the configuration to perform the conversion
    * @param e the [[Entity]] to convert
    * @return the collection of [[Report]] generated
    */
  def fromEntityToReport(e: E): R
}
