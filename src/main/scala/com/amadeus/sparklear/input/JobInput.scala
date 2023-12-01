package com.amadeus.sparklear.input

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.wrappers.JobWrapper

@Unstable
case class JobInput(w: JobWrapper) extends Input
