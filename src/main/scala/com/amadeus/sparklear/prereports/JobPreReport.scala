package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.raw.JobRawEvent
import com.amadeus.sparklear.raw.JobRawEvent.EndUpdate

@Unstable
case class JobPreReport(
  raw: JobRawEvent,
  endUpdate: EndUpdate
) extends PreReport
