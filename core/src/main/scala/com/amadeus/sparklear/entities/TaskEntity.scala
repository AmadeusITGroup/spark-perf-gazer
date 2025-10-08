package com.amadeus.sparklear.entities

import com.amadeus.sparklear.annotations.Unstable
import com.amadeus.sparklear.events.TaskEvent

@Unstable
case class TaskEntity(
  end: TaskEvent
) extends Entity
