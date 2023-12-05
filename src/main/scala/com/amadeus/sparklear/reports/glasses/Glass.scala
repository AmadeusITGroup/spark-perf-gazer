package com.amadeus.sparklear.reports.glasses

import com.amadeus.sparklear.reports.Report

/**
  * A filter that can be applied to a [[com.amadeus.sparklear.reports.Report]]
  */
trait Glass {
  def eligible(r: Report): Boolean
}
