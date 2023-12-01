package com.amadeus.sparklear.input

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.wrappers.StageWrapper

@Unstable
case class StageInput(w: StageWrapper) extends Input
