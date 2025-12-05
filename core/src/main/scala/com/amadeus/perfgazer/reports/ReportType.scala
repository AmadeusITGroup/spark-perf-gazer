package com.amadeus.perfgazer.reports

trait ReportType {
  def name: String
}

case object JobReportType extends ReportType {
  val name = "job"
}

case object StageReportType extends ReportType {
  val name = "stage"
}

case object TaskReportType extends ReportType {
  val name = "task"
}

case object SqlReportType extends ReportType {
  val name = "sql"
}

object ReportType {
  val standardTypes: Set[ReportType] = Set(SqlReportType, JobReportType, StageReportType, TaskReportType)
}
