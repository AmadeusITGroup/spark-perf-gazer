package com.amadeus.perfgazer.schema

import scala.annotation.StaticAnnotation

/** Documents a case class field for schema documentation generation.
  *
  * Used by the doc-generator to produce data model reference docs.
  *
  * @param description Human-readable description of the field
  * @param unit        Optional unit of measure (e.g. "ms", "ns", "bytes")
  */
class SchemaDoc(
  description: String,
  unit: String = ""
) extends StaticAnnotation
