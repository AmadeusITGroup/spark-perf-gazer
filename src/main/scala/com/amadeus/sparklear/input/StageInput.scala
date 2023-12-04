package com.amadeus.sparklear.input

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.collects.StageCollect

@Unstable
case class StageInput(w: StageCollect) extends Input
