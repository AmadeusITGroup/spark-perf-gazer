package com.amadeus.sparklear.collects

import com.amadeus.sparklear.input.Input

/**
  * A convenient wrapper of the raw metrics provided by Spark
  * @tparam T type of the output report that is generated from such data
  */
trait Collect[T <: Input]
