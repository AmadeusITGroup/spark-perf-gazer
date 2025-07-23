package com.amadeus.sparklear.reports

import com.amadeus.sparklear.entities.SqlEntity
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.execution.{ExtendedMode, FormattedMode, QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.ui.SparkInternal

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import scala.collection.JavaConverters._

case class SqlReport(
  details: String,
  nodes: Seq[SqlNode]
) extends Report

object SqlReport extends Translator[SqlEntity, SqlReport] {

  /** Convert a [[Entity]] P into a collection of [[Report]] R
    *
    * @param c the configuration to perform the conversion
    * @param p the [[Entity]] to buildNodes
    * @return the collection of [[Report]] generated
    */
  override def fromEntityToReport(report: SqlEntity): SqlReport = {
    val details = describe(SparkInternal.queryExecution(report.end))
    SqlReport(
      details = details,
      nodes = asNodes(report)
    )
  }

  private def buildNodes(
    jobName: String,
    baseCoordinates: String,
    sqlId: Long,
    plan: SparkPlan,
    parentNodeName: String
  ): Seq[SqlNode] = {

    val (children, metrics) = plan match {
      case a: AdaptiveSparkPlanExec => (a.finalPhysicalPlan.children, a.finalPhysicalPlan.metrics)
      case a: ShuffleQueryStageExec => (a.shuffle.children, a.shuffle.metrics)
      case x => (x.children, x.metrics)
    }

    val currNode = SqlNode(
      sqlId = sqlId,
      jobName = jobName,
      nodeName = s"(${plan.getTagValue(QueryPlan.OP_ID_TAG).mkString}) ${plan.nodeName}",
      coordinates = baseCoordinates,
      metrics = metrics.map(metricToKv),
      isLeaf = children.isEmpty,
      parentNodeName = parentNodeName
    )
    val childNode = children.zipWithIndex.flatMap { case (pi, i) =>
      buildNodes(jobName, baseCoordinates + s".${i}", sqlId, pi, plan.nodeName)
    }
    Seq(currNode) ++ childNode
  }

  private def describe(qe: QueryExecution): String = {
    val s = qe.explainString(ExtendedMode) // TODO check formatted as well
    s
  }

  private def metricToKv(s: (String, SQLMetric)): (String, String) =
    (s._2.name.getOrElse(s._1), s._2.value.toString)

  private def asNodes(sqlEntity: SqlEntity): Seq[SqlNode] = {
    val sqlId = sqlEntity.start.id
    val plan = SparkInternal.executedPlan(sqlEntity.end)
    val nodes = buildNodes(sqlEntity.start.description, "0", sqlId, plan, "")
    nodes
  }
}

object SqlGenericRecord extends GenericTranslator[SqlReport, GenericRecord] {
  override val reportSchema: Schema = new Schema.Parser()
    .parse("""
             |{
             | "type": "record",
             | "name": "Root",
             | "fields": [
             |   {"name": "details", "type": "string"},
             |   {"name": "nodes",
             |    "type": [
             |      "null",
             |      { "type": "array",
             |        "items": {
             |          "type": "record",
             |          "name": "Node",
             |          "fields": [
             |            { "name": "sqlId", "type": "long" },
             |            { "name": "jobName", "type": "string" },
             |            { "name": "nodeName", "type": "string" },
             |            { "name": "coordinates", "type": "string" },
             |            { "name": "metrics", "type": ["null", { "type": "map", "values": "string" } ] },
             |            { "name": "isLeaf", "type": "boolean" },
             |            { "name": "parentNodeName", "type": "string" }
             |          ]
             |        }
             |      }
             |    ]
             |   }
             |  ]
             |}
             |""".stripMargin)

  // Extract the array schema from the union
  private val SqlNodesFieldSchemaTmp = reportSchema.getField("nodes").schema()
  private val SqlNodesFieldSchema = SqlNodesFieldSchemaTmp.getType match {
    case Schema.Type.UNION => SqlNodesFieldSchemaTmp.getTypes.asScala.find(_.getType == Schema.Type.ARRAY).get
    case Schema.Type.ARRAY => SqlNodesFieldSchemaTmp
    case _ => throw new IllegalArgumentException("Expected array or union of array")
  }
  private val SqlNodeSchema = SqlNodesFieldSchema.getElementType

  override def fromReportToGenericRecord(r: SqlReport): GenericRecord = {
    val record = new GenericData.Record(reportSchema)
    val nodes = new GenericData.Array(SqlNodesFieldSchema, r.nodes.map { n =>
      val node = new GenericData.Record(SqlNodeSchema)
      node.put("sqlId", n.sqlId)
      node.put("jobName", n.jobName)
      node.put("nodeName", n.nodeName)
      node.put("coordinates", n.coordinates)
      node.put("metrics", n.metrics.asJava)
      node.put("isLeaf", n.isLeaf)
      node.put("parentNodeName", n.parentNodeName)
      node
    }.toList.asJava )

    record.put("details", r.details)
    record.put("nodes", nodes)
    record
  }
}