package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.SqlCollect

@Unstable
case class SqlPreReport(w: SqlCollect, m: Map[Long, Long]) extends PreReport
