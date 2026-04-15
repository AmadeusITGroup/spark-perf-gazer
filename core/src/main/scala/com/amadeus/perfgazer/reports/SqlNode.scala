package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.schema.SchemaDoc

case class SqlNode(
  @SchemaDoc(value = "SQL execution this node belongs to")
  sqlId: Long,
  @SchemaDoc(value = "Name of the job that triggered this SQL")
  jobName: String,
  @SchemaDoc(value = "Spark physical plan operator name")
  nodeName: String,
  @SchemaDoc(value = "Dot-separated position in the plan tree, e.g. '0.1.2'")
  coordinates: String,
  @SchemaDoc(value = "Operator metrics as key-value pairs")
  metrics: Map[String, String],
  @SchemaDoc(value = "True if this node has no children in the plan tree")
  isLeaf: Boolean,
  @SchemaDoc(value = "Name of the parent operator in the plan tree")
  parentNodeName: String
)
