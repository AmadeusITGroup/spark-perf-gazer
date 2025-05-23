package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.raw.StageRawEvent

@Unstable
case class StagePreReport(
  raw: StageRawEvent
) extends PreReport
