package com.amadeus.sparklear.entities

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.events.StageEvent

@Unstable
case class StageEntity(
  end: StageEvent
) extends Entity
