package com.amadeus.sparklear.reports.filters

import com.amadeus.sparklear.reports.Report

/**
  * A filter that can be applied to a [[Report]]
  *
  */
trait Filter {
  /**
    * Tells if the report is eligible as per the current filter
    *
    * Convention: a filter typically applies to a given type of [[Report]], for non-supported
    * [[Report]] this method must return true
    * @param r report being judged as eligible (to be kept) or not (to be discarded)
    * @return true if the report is eligible (to be kept)
    */
  def eligible(r: Report): Boolean
}
