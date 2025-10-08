package com.amadeus.sparklear.events

import com.amadeus.sparklear.entities.Entity

/**
  * A convenient wrapper of the raw Events provided by Spark via the listeners
  * @tparam T type of the output report that is generated from such data
  */
trait Event[T <: Entity]
