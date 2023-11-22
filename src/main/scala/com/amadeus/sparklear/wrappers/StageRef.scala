package com.amadeus.sparklear.wrappers

case class StageRef(id: Int, nroTasks: Int) {
  override def toString(): String = s"ID=${id} TASKS=${nroTasks}"
}
