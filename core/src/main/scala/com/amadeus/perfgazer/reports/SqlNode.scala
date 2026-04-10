package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.schema.SchemaDoc

case class SqlNode(
  @SchemaDoc("SQL execution this node belongs to")
  sqlId: Long,
  @SchemaDoc("Name of the job that triggered this SQL")
  jobName: String,
  @SchemaDoc("Spark physical plan operator name")
  nodeName: String,
  @SchemaDoc("Dot-separated position in the plan tree, e.g. '0.1.2'")
  coordinates: String,
  @SchemaDoc("Operator metrics as key-value pairs")
  metrics: Map[String, String],
  @SchemaDoc("True if this node has no children in the plan tree")
  isLeaf: Boolean,
  @SchemaDoc("Name of the parent operator in the plan tree")
  parentNodeName: String
)
