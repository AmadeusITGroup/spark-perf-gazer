package com.amadeus.sparklear.entities

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.events.JobEvent
import com.amadeus.sparklear.events.JobEvent.EndUpdate

@Unstable
case class JobEntity(
  start: JobEvent,
  end: EndUpdate
) extends Entity
