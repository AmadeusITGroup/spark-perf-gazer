package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.Report

/**
  * A filter that can be applied to a [[Report]]
  *
  */
trait Glass {
  /**
    * Tells if the report is eligible as per the current glass
    *
    * Convention: a glass typically applies to a given type of [[Report]], for non-supported
    * [[Report]] this method must return true
    * @param r report being judged as eligible (to be kept) or not (to be discarded)
    * @return true if the report is eligible (to be kept)
    */
  def eligible(r: Report): Boolean
}
