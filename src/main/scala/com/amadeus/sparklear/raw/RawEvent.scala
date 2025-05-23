package com.amadeus.sparklear.raw

import com.amadeus.sparklear.prereports.PreReport

/**
  * A convenient wrapper of the raw metrics provided by Spark
  * @tparam T type of the output report that is generated from such data
  */
trait RawEvent[T <: PreReport]
