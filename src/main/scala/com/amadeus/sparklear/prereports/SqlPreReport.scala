package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.SqlCollect

@Unstable
case class SqlPreReport(
  collect: SqlCollect,
  metrics: Map[Long, Long]
) extends PreReport
