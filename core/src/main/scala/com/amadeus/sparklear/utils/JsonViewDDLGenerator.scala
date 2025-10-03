package com.amadeus.sparklear.utils

object JsonViewDDLGenerator {

  /** Generate the CREATE OR REPLACE TEMPORARY VIEW DDL for a partitioned JSON file path.
    *
    * @param fullPath  A concrete file path pointing to one actual json file inside partition dirs.
    * @param reportName  The report name, that is used as view name too (e.g. "job", "sql", ...).
    */
  def generateViewDDL(fullPath: String, reportName: String): String = {
    val norm = fullPath.replace('\\', '/').stripSuffix("/") // normalize
    val fileName = norm.substring(norm.lastIndexOf('/') + 1)

    // Split all path segments
    val segments = norm.split('/').filter(_.nonEmpty).toList

    // Index of file
    val fileIdx = segments.lastIndexWhere(_ == fileName)
    require(fileIdx >= 0, s"Cannot locate file name in path: $fullPath")

    // Heuristic: basePath is everything up to (but not including) the first partition-style segment
    // A partition segment is assumed to contain '=' (e.g., date=2025-09-10).
    val firstPartitionIdx =
      segments.indexWhere(s => s.contains('=') && s != fileName)

    val baseEndExclusive =
      if (firstPartitionIdx == -1) { // no partition dirs detected
        fileIdx
      } else {
        firstPartitionIdx
      }

    val basePathSegments = segments.take(baseEndExclusive)
    val basePath = "/" + (if (basePathSegments.isEmpty) "" else basePathSegments.mkString("/") + "/")

    // Partition directory segments (between basePath and the file)
    val partitionSegments =
      segments.slice(baseEndExclusive, fileIdx).filter(_.nonEmpty)

    val starPathPart =
      if (partitionSegments.isEmpty) { // no partitions
        ""
      } else {
        List.fill(partitionSegments.size)("*").mkString("", "/", "/")
      }

    // Always use the pattern "${viewName}-reports-*.json" for the file name
    val fileNameWithWildcard = s"${reportName}-reports-*.json"
    val globPath = s"$basePath$starPathPart$fileNameWithWildcard"

    val ddl =
      s"""|CREATE OR REPLACE TEMPORARY VIEW $reportName
          |USING json
          |OPTIONS (
          |  path \"$globPath\",
          |  basePath \"$basePath\"
          |);""".stripMargin
    ddl
  }

  // Demo
  def main(args: Array[String]): Unit = {
    val sample = "/tmp/listener/date=2025-09-10/cluster=111/id=ffff/level=ggg/sql-reports-1234.json"
    println(generateViewDDL(sample, "sql"))
  }
}
