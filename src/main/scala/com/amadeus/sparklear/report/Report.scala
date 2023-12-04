package com.amadeus.sparklear.report

import com.amadeus.sparklear.converters.Reporter.StringReport
import com.amadeus.sparklear.report.glasses.Glass

trait Report {
  def asString: StringReport
  def eligible(g: Glass): Boolean
}
