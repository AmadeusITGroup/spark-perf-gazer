package com.amadeus.sparklear.reports.converters

import com.amadeus.sparklear.reports.Report.StringReport

object Serializer {
  trait Report {
    def asString: StringReport
  }
}
