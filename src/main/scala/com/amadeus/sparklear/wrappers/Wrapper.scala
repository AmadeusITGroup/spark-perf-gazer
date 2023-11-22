package com.amadeus.sparklear.wrappers

import com.amadeus.sparklear.reports.Report

/**
  * A convenient wrapper of the raw metrics provided by Spark
  * @tparam T type of the output report that is generated from such data
  */
trait Wrapper[T <: Report] {
  def toReport(): T
}
