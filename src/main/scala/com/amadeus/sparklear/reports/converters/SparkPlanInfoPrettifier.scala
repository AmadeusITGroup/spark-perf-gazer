package com.amadeus.sparklear.reports.converters

import org.apache.spark.sql.execution.SparkPlanInfo
import org.apache.spark.sql.execution.metric.SQLMetricInfo
import org.apache.spark.sql.execution.ui.SparkPlanGraph

object SparkPlanInfoPrettifier {

  private def resolveMetric(i: SQLMetricInfo, metrics: Map[Long, Long]) = {
    s"'${i.name.replace(" ", "_")}'=${metrics.get(i.accumulatorId).mkString}"
  }

  def prettify(planInfo: SparkPlanInfo, metrics: Map[Long, Long], indent: String = ""): String = {
    val builder = new StringBuilder()
    builder.append(s"${indent}Operator ${planInfo.nodeName} | ${planInfo.simpleString}\n")
    if (planInfo.metadata.nonEmpty) {
      builder.append(s"${indent}- Metadata: ${planInfo.metadata}\n")
    }
    if (planInfo.metrics.nonEmpty) {
      builder.append(s"${indent}- Metrics: ${planInfo.metrics.map(i => resolveMetric(i, metrics)).mkString(",")}\n")
    }
    // Recursively prettify children
    planInfo.children.foreach { childInfo =>
      builder.append(prettify(childInfo, metrics, s"$indent      "))
    }
    val s1 = builder.toString()
    s1
  }
}

