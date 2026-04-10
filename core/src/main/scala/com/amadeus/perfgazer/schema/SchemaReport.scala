package com.amadeus.perfgazer.schema

import scala.annotation.StaticAnnotation

/** Marks a Report case class and binds it to its SQL view name.
  *
  * Used by the doc-generator to produce data model reference docs.
  *
  * @param viewName    The SQL temporary view name (e.g. "job", "sql", "stage", "task")
  * @param description Human-readable description of the report/view
  */
class SchemaReport(
  viewName: String,
  description: String
) extends StaticAnnotation
