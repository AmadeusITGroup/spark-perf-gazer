package com.amadeus.sparklear.input

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.SqlCollect

@Unstable
case class SqlInput(w: SqlCollect, m: Map[Long, Long]) extends Input
