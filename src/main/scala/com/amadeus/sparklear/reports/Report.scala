package com.amadeus.sparklear.reports

import com.amadeus.sparklear.translators.Translator.StringReport
import com.amadeus.sparklear.reports.glasses.Glass

trait Report {
  def asStringReport: StringReport
  def eligible(g: Glass): Boolean
}
