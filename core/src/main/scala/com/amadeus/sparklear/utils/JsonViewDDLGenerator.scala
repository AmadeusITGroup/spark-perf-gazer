package com.amadeus.sparklear.utils

object JsonViewDDLGenerator {

  /** Generate the CREATE OR REPLACE TEMPORARY VIEW DDL for a partitioned JSON file path.
    *
    * Example of a partitioned path:
    *
    * /tmp/listener/date=2025-09-10/cluster=my-cluster/customer=my-customer/sql-reports-1234.json
    *
    * The generated DDL will be:
    * {{{
    * CREATE OR REPLACE TEMPORARY VIEW sql
    * USING json
    * OPTIONS (
    *  path "/tmp/listener/date=2025-10-10/cluster=my-cluster/customer=my-customer/sql-reports-*.json",
    *  basePath "/tmp/listener/"
    *  )
    *  }}}
    *
    * @param fullPath  A concrete file path pointing to one actual json file inside partition dirs.
    * @param reportName  The report name, that is used as view name too (e.g. "job", "sql", ...).
    */
  def generateViewDDL(fullPath: String, reportName: String): String = {
    val normalizedFullPath = fullPath.replace('\\', '/').stripSuffix("/")
    val fileName = normalizedFullPath.substring(normalizedFullPath.lastIndexOf('/') + 1)
    if (fileName.isEmpty || !fileName.endsWith(".json")) {
      throw new IllegalArgumentException(s"Path does not contain a valid JSON file name: $fullPath")
    }

    // Split all path segments
    val segments = normalizedFullPath.split('/').filter(_.nonEmpty).toList

    // Index of file
    val fileIdx = segments.lastIndexWhere(_ == fileName)
    require(fileIdx >= 0, s"Cannot locate file name in path: $fullPath")

    // Rule: basePath is everything up to (but not including) that partition-style segment
    // starting from which there are only more partition-style segments and the filename at the end.
    // Example: /base/a=10/something/b=10/c=30/file.json -> basePath is /base/a=10/something/
    def isPartitionSegment(s: String): Boolean = s.contains('=')
    val lastNonPartitionIdx = segments.zipWithIndex.reverse
      .collectFirst {
        case (seg, idx) if idx < fileIdx && !isPartitionSegment(seg) => idx
      }
      .getOrElse(-1)
    val baseEndExclusive =
      if (lastNonPartitionIdx == -1) {
        // No non-partition segments found before file, use first partition
        segments.indexWhere(s => isPartitionSegment(s) && s != fileName) match {
          case -1 => fileIdx // no partitions at all
          case idx => idx
        }
      } else {
        lastNonPartitionIdx + 1
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
