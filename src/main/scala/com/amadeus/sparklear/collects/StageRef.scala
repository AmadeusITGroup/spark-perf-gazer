package com.amadeus.sparklear.collects

case class StageRef(id: Int, nroTasks: Int) {
  override def toString(): String = s"ID=${id} TASKS=${nroTasks}"
}
