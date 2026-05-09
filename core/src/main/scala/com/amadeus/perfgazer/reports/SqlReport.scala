package com.amadeus.perfgazer.reports

import com.amadeus.perfgazer.events.SqlEvent
import com.amadeus.perfgazer.schema.{ColumnDoc, TableDoc}
import org.apache.spark.sql.execution.{ExtendedMode, QueryExecution, SparkPlan, WholeStageCodegenExec, InputAdapter, CodegenSupport}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ui.{SparkInternal, SparkListenerSQLExecutionEnd}

@TableDoc(name = "sql", description = "SQL query execution report with SQL plans (logical, physical, ...) and their node metrics. One row per completed SQL execution.")
case class SqlReport(
  @ColumnDoc(description = "Unique SQL execution identifier")
  sqlId: Long,
  @ColumnDoc(description = "SQL query description")
  description: String,
  @ColumnDoc(description = "Extended query execution plan")
  details: String,
  @ColumnDoc(description = "Physical plan nodes with execution metrics")
  nodes: Seq[SqlNode]
) extends Report {
  override def reportType: ReportType = SqlReportType
}

object SqlReport {

  /** Create a SqlReport
    *
    * @param start the SqlEvent for SQL execution start
    * @param end the SparkListenerSQLExecutionEnd for SQL execution end
    * @return the SqlReport generated
    */
  def apply(start: SqlEvent, end: SparkListenerSQLExecutionEnd): SqlReport = {
    val details = describe(SparkInternal.queryExecution(end))
    SqlReport(
      sqlId = start.id,
      description = start.description,
      details = details,
      nodes = asNodes(start, end)
    )
  }

  /** True for operators that can live inside a WSCG region. */
  private def supportsCodegen(p: SparkPlan): Boolean = p.isInstanceOf[CodegenSupport]

  /** InputAdapter is codegen-capable, but its child is outside the WSCG region. */
  private def isInputAdapter(p: SparkPlan): Boolean = p.isInstanceOf[InputAdapter]

  private def buildNodes(
    jobName: String,
    baseCoordinates: String,
    sqlId: Long,
    plan: SparkPlan,
    parentNodeName: String,
    parentCodegenId: Option[Int]
  ): Seq[SqlNode] = {

    val (children, metrics) = plan match {
      case a: AdaptiveSparkPlanExec => (a.finalPhysicalPlan.children, a.finalPhysicalPlan.metrics)
      case a: ShuffleQueryStageExec => (a.shuffle.children, a.shuffle.metrics)
      case b: BroadcastQueryStageExec => (b.plan.children, b.plan.metrics)
      case x => (x.children, x.metrics)
    }

    // If this node is a WSCG root, fetch the current codegenStageId
    val (enteredCodegenId, isWholeStageRoot) = plan match {
      case w: WholeStageCodegenExec => (Some(w.codegenStageId), true)
      case _                        => (parentCodegenId       , false)
    }

    // Attach id ONLY if the node is a WSCG root OR it supports codegen AND we're inside a WSCG region
    val currCodegenId: Option[Int] =
      if (isWholeStageRoot || (supportsCodegen(plan) && enteredCodegenId.isDefined)) {
        enteredCodegenId
      } else {
        None
      }

    // Codegen propagation rule to children:
    // - If WholeStageCodegenExec: propagate id into children
    // - If other CodegenSupport: carry id
    // - If InputAdapter: DO NOT propagate past its child (boundary downwards)
    // - Otherwise (not codegen): boundary -> reset id
    val nextCodegenId: Option[Int] =
      if (isWholeStageRoot || (supportsCodegen(plan) && !isInputAdapter(plan))) {
        enteredCodegenId
      } else {
        None
      }

    val currNodeName = s"${plan.nodeName} (${plan.id})"
    val currNode = SqlNode(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = currNodeName,
      coordinates = baseCoordinates,
      metrics = metrics.filter(_._2.isRegistered).map(metricToKv),
      isLeaf = children.isEmpty,
      parentNodeName = parentNodeName,
      codegenId = currCodegenId,
      isWholeStageRoot = isWholeStageRoot,
      codegenAccumulatorIds = metrics.filter(_._2.isRegistered).map(m => m._2.id).toSeq
    )
    val childNode = children.zipWithIndex.flatMap { case (pi, i) =>
      buildNodes(jobName, baseCoordinates + s".$i", sqlId, pi, currNodeName, nextCodegenId)
    }
    Seq(currNode) ++ childNode
  }

  private def describe(qe: QueryExecution): String = {
    qe.explainString(ExtendedMode)
  }

  private def metricToKv(s: (String, SQLMetric)): (String, String) =
    ( s._2.name.getOrElse(
        // $COVERAGE-OFF$ A SQLMetric should always have a name as long as it has been registered
        s._1
        // $COVERAGE-ON$
      ), s._2.value.toString
    )

  private def asNodes(start: SqlEvent, end: SparkListenerSQLExecutionEnd): Seq[SqlNode] = {
    val sqlId = start.id
    val plan = SparkInternal.executedPlan(end)
    val nodes = buildNodes(start.description, "0", sqlId, plan, "", None)

    // Aggregate codegenAccumulatorIds by codegenId
    val codegenIdToAccIds: Map[Int, Seq[Long]] = nodes
      .filter(_.codegenId.isDefined)
      .groupBy(_.codegenId.get)
      .mapValues(_.flatMap(_.codegenAccumulatorIds).distinct)
      .toMap

    // Update nodes with aggregated codegenAccumulatorIds
    nodes.map { node =>
      node.codegenId match {
        case Some(id) =>
          node.copy(codegenAccumulatorIds = codegenIdToAccIds(id))
        case None =>
          node
      }
    }
  }
}
