package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.schema.ColumnDoc

case class SqlNode(
  @ColumnDoc(description = "SQL execution this node belongs to")
  sqlId: Long,
  @ColumnDoc(description = "Name of the job that triggered this SQL")
  jobName: String,
  @ColumnDoc(description = "Spark physical plan operator name")
  nodeName: String,
  @ColumnDoc(description = "Dot-separated position in the plan tree, e.g. '0.1.2'")
  coordinates: String,
  @ColumnDoc(description = "Operator metrics as key-value pairs")
  metrics: Map[String, String],
  @ColumnDoc(description = "True if this node has no children in the plan tree")
  isLeaf: Boolean,
  @ColumnDoc(description = "Name of the parent operator in the plan tree")
  parentNodeName: String,
  @ColumnDoc(description = "Whole-Stage Codegen stage ID, if this node participates in codegen")
  codegenId: Option[Int] = None,
  @ColumnDoc(description = "True when this node is a WholeStageCodegenExec root")
  isWholeStageRoot: Boolean = false,
  @ColumnDoc(description = "Accumulator IDs for the Whole-Stage Codegen region")
  codegenAccumulatorIds: Seq[Long] = Seq.empty
)
