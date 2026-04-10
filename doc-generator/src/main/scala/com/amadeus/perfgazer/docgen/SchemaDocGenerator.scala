package com.amadeus.perfgazer.docgen

import scala.meta._
import java.io.{File, PrintWriter}

/** Parses annotated case classes and generates data model documentation.
  *
  * Reads Scala source files from the reports package, extracts @SchemaReport
  * and @SchemaDoc annotations, and emits:
  *   - A Markdown file for human consumption (MkDocs)
  *   - A JSON file for agent/tool consumption
  */
object SchemaDocGenerator {

  /** A documented field extracted from a case class parameter. */
  case class FieldDoc(
    name: String,
    scalaType: String,
    sqlType: String,
    unit: String,
    description: String,
    nestedFields: Seq[FieldDoc]
  )

  /** A documented report/view extracted from an annotated case class. */
  case class ViewDoc(
    viewName: String,
    className: String,
    description: String,
    fields: Seq[FieldDoc]
  )

  // Scala type -> Spark SQL type mapping
  private val primitiveTypeMap: Map[String, String] = Map(
    "Int"     -> "INT",
    "Long"    -> "BIGINT",
    "String"  -> "STRING",
    "Boolean" -> "BOOLEAN",
    "Double"  -> "DOUBLE",
    "Float"   -> "FLOAT",
    "Short"   -> "SMALLINT",
    "Byte"    -> "TINYINT"
  )

  def main(args: Array[String]): Unit = {
    val reportsDir = args.headOption.getOrElse("core/src/main/scala/com/amadeus/perfgazer/reports")
    val mdOutput = if (args.length > 1) args(1) else "docs/user_guide/data_model.md"
    val jsonOutput = if (args.length > 2) args(2) else "docs/schema/perfgazer-schema.json"

    val sourceFiles = new File(reportsDir).listFiles().filter(_.getName.endsWith(".scala")).toSeq
    val allTrees: Seq[(String, scala.meta.Source)] = sourceFiles.map { f =>
      val content = scala.io.Source.fromFile(f, "UTF-8").mkString
      val tree = dialects.Scala213(content).parse[scala.meta.Source].get
      (f.getName, tree)
    }

    // First pass: collect all case classes (needed to resolve nested types)
    val allCaseClasses: Map[String, Defn.Class] = allTrees.flatMap { case (_, tree) =>
      tree.collect {
        case c: Defn.Class if c.mods.exists(_.is[Mod.Case]) =>
          c.name.value -> c
      }
    }.toMap

    // Second pass: extract annotated views
    val views: Seq[ViewDoc] = allTrees.flatMap { case (_, tree) =>
      tree.collect {
        case c: Defn.Class if c.mods.exists(_.is[Mod.Case]) && hasSchemaReport(c) =>
          extractViewDoc(c, allCaseClasses)
      }
    }.sortBy(_.viewName)

    // Emit outputs
    new File(new File(mdOutput).getParent).mkdirs()
    new File(new File(jsonOutput).getParent).mkdirs()
    writeMarkdown(views, mdOutput)
    writeJson(views, jsonOutput)

    println(s"Generated $mdOutput (${views.size} views)")
    println(s"Generated $jsonOutput")
  }

  private def hasSchemaReport(c: Defn.Class): Boolean =
    c.mods.exists {
      case Mod.Annot(Init.After_4_6_0(Type.Name("SchemaReport"), _, _)) => true
      case _ => false
    }

  /** Extract a positional string argument from annotation arg clauses. */
  private def extractAnnotString(argClauses: Seq[Term.ArgClause], index: Int): String = {
    val allArgs = argClauses.flatMap(_.values)
    allArgs.lift(index) match {
      case Some(Lit.String(s)) => s
      case _ => ""
    }
  }

  /** Extract a named string argument from annotation arg clauses. */
  private def extractNamedArg(argClauses: Seq[Term.ArgClause], name: String): String = {
    val allArgs = argClauses.flatMap(_.values)
    allArgs.collectFirst {
      case Term.Assign(Term.Name(`name`), Lit.String(s)) => s
    }.getOrElse("")
  }

  private def extractSchemaReportArgs(c: Defn.Class): (String, String) = {
    c.mods.collectFirst {
      case Mod.Annot(Init.After_4_6_0(Type.Name("SchemaReport"), _, argClauses)) =>
        (extractAnnotString(argClauses, 0), extractAnnotString(argClauses, 1))
    }.getOrElse(("", ""))
  }

  private def extractSchemaDocArgs(mods: List[Mod]): (String, String) = {
    mods.collectFirst {
      case Mod.Annot(Init.After_4_6_0(Type.Name("SchemaDoc"), _, argClauses)) =>
        val desc = extractAnnotString(argClauses, 0)
        val unit = {
          val positional = extractAnnotString(argClauses, 1)
          if (positional.nonEmpty) positional else extractNamedArg(argClauses, "unit")
        }
        (desc, unit)
    }.getOrElse(("", ""))
  }

  private def scalaTypeToSql(scalaType: String, allCaseClasses: Map[String, Defn.Class]): String = {
    scalaType match {
      case t if primitiveTypeMap.contains(t) => primitiveTypeMap(t)
      case s if s.startsWith("Option[") =>
        val inner = s.stripPrefix("Option[").stripSuffix("]")
        scalaTypeToSql(inner, allCaseClasses)
      case s if s.startsWith("Seq[") || s.startsWith("List[") || s.startsWith("Array[") =>
        val inner = s.substring(s.indexOf('[') + 1, s.lastIndexOf(']'))
        if (allCaseClasses.contains(inner)) {
          val nestedFields = extractFieldDocs(allCaseClasses(inner), allCaseClasses)
          val structFields = nestedFields.map(f => s"${f.name}: ${f.sqlType}").mkString(", ")
          s"ARRAY<STRUCT<$structFields>>"
        } else {
          s"ARRAY<${scalaTypeToSql(inner, allCaseClasses)}>"
        }
      case s if s.startsWith("Map[") =>
        val inner = s.stripPrefix("Map[").stripSuffix("]")
        val parts = splitTopLevelComma(inner)
        if (parts.size == 2) {
          s"MAP<${scalaTypeToSql(parts(0).trim, allCaseClasses)}, ${scalaTypeToSql(parts(1).trim, allCaseClasses)}>"
        } else {
          "MAP<STRING, STRING>"
        }
      case t if allCaseClasses.contains(t) =>
        val nestedFields = extractFieldDocs(allCaseClasses(t), allCaseClasses)
        val structFields = nestedFields.map(f => s"${f.name}: ${f.sqlType}").mkString(", ")
        s"STRUCT<$structFields>"
      case other => other.toUpperCase
    }
  }

  /** Split a string by commas, respecting nested brackets. */
  private def splitTopLevelComma(s: String): Seq[String] = {
    var depth = 0
    val parts = scala.collection.mutable.ArrayBuffer[String]()
    val current = new StringBuilder
    for (c <- s) {
      c match {
        case '[' | '<' | '(' => depth += 1; current += c
        case ']' | '>' | ')' => depth -= 1; current += c
        case ',' if depth == 0 =>
          parts += current.toString
          current.clear()
        case _ => current += c
      }
    }
    if (current.nonEmpty) parts += current.toString
    parts.toSeq
  }

  private def typeToString(t: Type): String = t match {
    case Type.Name(name) => name
    case Type.Apply.After_4_6_0(tpe, argClause) =>
      s"${typeToString(tpe)}[${argClause.values.map(typeToString).mkString(", ")}]"
    case Type.Select(qual, name) => s"$qual.$name"
    case _ => t.syntax
  }

  private def extractFieldDocs(c: Defn.Class, allCaseClasses: Map[String, Defn.Class]): Seq[FieldDoc] = {
    c.ctor.paramClauses.flatMap(_.values).map { param =>
      val (desc, unit) = extractSchemaDocArgs(param.mods)
      val scalaType = param.decltpe.map(typeToString).getOrElse("Unknown")
      val sqlType = scalaTypeToSql(scalaType, allCaseClasses)

      // Extract nested fields for case class references
      val innerTypeName = scalaType match {
        case s if s.startsWith("Seq[") => s.stripPrefix("Seq[").stripSuffix("]")
        case s if s.startsWith("List[") => s.stripPrefix("List[").stripSuffix("]")
        case s => s
      }
      val nestedFields = allCaseClasses.get(innerTypeName) match {
        case Some(nestedClass) if innerTypeName != c.name.value =>
          extractFieldDocs(nestedClass, allCaseClasses)
        case _ => Seq.empty
      }

      FieldDoc(
        name = param.name.value,
        scalaType = scalaType,
        sqlType = sqlType,
        unit = unit,
        description = desc,
        nestedFields = nestedFields
      )
    }
  }

  private def extractViewDoc(c: Defn.Class, allCaseClasses: Map[String, Defn.Class]): ViewDoc = {
    val (viewName, viewDesc) = extractSchemaReportArgs(c)
    val fields = extractFieldDocs(c, allCaseClasses)
    ViewDoc(viewName, c.name.value, viewDesc, fields)
  }

  // ── Markdown emitter ──────────────────────────────────────────────────

  private def writeMarkdown(views: Seq[ViewDoc], path: String): Unit = {
    val pw = new PrintWriter(new File(path))
    try {
      pw.println("# Data Model Reference")
      pw.println()
      pw.println("PerfGazer writes reports as JSON files. Each report type maps to a SQL temporary view.")
      pw.println("The schemas below describe the structure of each view.")
      pw.println()

      for (view <- views) {
        pw.println(s"## `${view.viewName}` view")
        pw.println()
        pw.println(view.description)
        pw.println()
        pw.println("| Column | SQL Type | Unit | Description |")
        pw.println("|--------|----------|------|-------------|")
        for (f <- view.fields) {
          val unitStr = if (f.unit.nonEmpty) f.unit else ""
          val sqlTypeEscaped = escapeMdPipe(f.sqlType)
          pw.println(s"| ${f.name} | `$sqlTypeEscaped` | $unitStr | ${f.description} |")
        }
        pw.println()

        // Render nested types as sub-sections
        for (f <- view.fields if f.nestedFields.nonEmpty) {
          val nestedTypeName = f.scalaType match {
            case s if s.startsWith("Seq[") => s.stripPrefix("Seq[").stripSuffix("]")
            case s if s.startsWith("List[") => s.stripPrefix("List[").stripSuffix("]")
            case s => s
          }
          pw.println(s"### `$nestedTypeName`")
          pw.println()
          pw.println("| Column | SQL Type | Unit | Description |")
          pw.println("|--------|----------|------|-------------|")
          for (nf <- f.nestedFields) {
            val unitStr = if (nf.unit.nonEmpty) nf.unit else ""
            val sqlTypeEscaped = escapeMdPipe(nf.sqlType)
            pw.println(s"| ${nf.name} | `$sqlTypeEscaped` | $unitStr | ${nf.description} |")
          }
          pw.println()
        }
      }
    } finally {
      pw.close()
    }
  }

  private def escapeMdPipe(s: String): String = s.replace("|", "\\|")

  // ── JSON emitter ──────────────────────────────────────────────────────

  private def writeJson(views: Seq[ViewDoc], path: String): Unit = {
    val pw = new PrintWriter(new File(path))
    try {
      pw.println("{")
      pw.println("""  "project": "PerfGazer",""")
      pw.println("""  "description": "Schema reference for PerfGazer report views. Each view corresponds to a SQL temporary view created by JsonSink.",""")
      pw.println("""  "views": [""")

      for ((view, vi) <- views.zipWithIndex) {
        pw.println("    {")
        pw.println(s"""      "name": "${escJson(view.viewName)}",""")
        pw.println(s"""      "description": "${escJson(view.description)}",""")
        pw.println("""      "fields": [""")

        for ((f, fi) <- view.fields.zipWithIndex) {
          val comma = if (fi < view.fields.size - 1) "," else ""
          if (f.nestedFields.isEmpty) {
            val unitPart = if (f.unit.nonEmpty) s""", "unit": "${escJson(f.unit)}"""" else ""
            pw.println(s"""        { "name": "${escJson(f.name)}", "type": "${escJson(f.sqlType)}"$unitPart, "description": "${escJson(f.description)}" }$comma""")
          } else {
            pw.println(s"""        { "name": "${escJson(f.name)}", "type": "${escJson(f.sqlType)}", "description": "${escJson(f.description)}",""")
            pw.println("""          "nestedSchema": {""")
            val nestedTypeName = f.scalaType match {
              case s if s.startsWith("Seq[") => s.stripPrefix("Seq[").stripSuffix("]")
              case s if s.startsWith("List[") => s.stripPrefix("List[").stripSuffix("]")
              case s => s
            }
            pw.println(s"""            "name": "$nestedTypeName",""")
            pw.println("""            "fields": [""")
            for ((nf, nfi) <- f.nestedFields.zipWithIndex) {
              val ncomma = if (nfi < f.nestedFields.size - 1) "," else ""
              val nunitPart = if (nf.unit.nonEmpty) s""", "unit": "${escJson(nf.unit)}"""" else ""
              pw.println(s"""              { "name": "${escJson(nf.name)}", "type": "${escJson(nf.sqlType)}"$nunitPart, "description": "${escJson(nf.description)}" }$ncomma""")
            }
            pw.println("            ]")
            pw.println("          }")
            pw.println(s"        }$comma")
          }
        }

        pw.println("      ]")
        val viewComma = if (vi < views.size - 1) "," else ""
        pw.println(s"    }$viewComma")
      }

      pw.println("  ]")
      pw.println("}")
    } finally {
      pw.close()
    }
  }

  private def escJson(s: String): String =
    s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
}
