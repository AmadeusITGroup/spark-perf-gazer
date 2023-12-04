package com.amadeus.sparklear.prereports

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.JobCollect
import com.amadeus.sparklear.collects.JobCollect.EndUpdate

@Unstable
case class JobPreReport(w: JobCollect, e: EndUpdate) extends PreReport
