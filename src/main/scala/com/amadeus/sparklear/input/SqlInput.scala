package com.amadeus.sparklear.input

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.wrappers.SqlWrapper

@Unstable
case class SqlInput(w: SqlWrapper, m: Map[Long, Long]) extends Input
