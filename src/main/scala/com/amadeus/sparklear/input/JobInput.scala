package com.amadeus.sparklear.input

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.JobCollect
import com.amadeus.sparklear.collects.JobCollect.EndUpdate

@Unstable
case class JobInput(w: JobCollect, e: EndUpdate) extends Input
