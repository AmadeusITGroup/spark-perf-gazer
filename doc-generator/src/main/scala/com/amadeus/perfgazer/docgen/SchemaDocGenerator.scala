package com.amadeus.perfgazer.docgen

import com.amadeus.perfgazer.schema.{SchemaDoc, SchemaReport}

import java.io.{File, PrintWriter}
import scala.reflect.runtime.universe._

/** Reflects on annotated case classes and generates data model documentation.
  *
  * Loads compiled report classes, reads @SchemaReport and @SchemaDoc annotations
  * via Java reflection, and uses Scala reflection for accurate generic type info.
  * Emits:
  *   - A Markdown file for human consumption (MkDocs)
  *   - A JSON file for agent/tool consumption
  */
object SchemaDocGenerator {

  /** A documented field extracted from a case class constructor parameter. */
  case class FieldDoc(
    name: String,
    scalaType: String,
    sqlType: String,
    unit: String,
    description: String,
    nestedFields: Seq[FieldDoc]
  )

  /** A documented report/view extracted from an annotated class. */
  case class ViewDoc(
    viewName: String,
    className: String,
    description: String,
    fields: Seq[FieldDoc]
  )

  // Scala type name -> Spark SQL type mapping
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

  private val mirror = runtimeMirror(getClass.getClassLoader)

  /** All report classes to document. Add new report classes here. */
  private val reportClasses: Seq[Class[_]] = Seq(
    classOf[com.amadeus.perfgazer.reports.JobReport],
    classOf[com.amadeus.perfgazer.reports.SqlReport],
    classOf[com.amadeus.perfgazer.reports.StageReport],
    classOf[com.amadeus.perfgazer.reports.TaskReport]
  )

  def main(args: Array[String]): Unit = {
    val mdOutput = args.headOption.getOrElse("docs/user_guide/data_model.md")
    val jsonOutput = if (args.length > 1) args(1) else "docs/schema/perfgazer-schema.json"

    val views: Seq[ViewDoc] = reportClasses
      .filter(_.isAnnotationPresent(classOf[SchemaReport]))
      .map(extractViewDoc)
      .sortBy(_.viewName)

    // Emit outputs
    new File(new File(mdOutput).getParent).mkdirs()
    new File(new File(jsonOutput).getParent).mkdirs()
    writeMarkdown(views, mdOutput)
    writeJson(views, jsonOutput)

    println(s"Generated $mdOutput (${views.size} views)")
    println(s"Generated $jsonOutput")
  }

  private def extractViewDoc(clazz: Class[_]): ViewDoc = {
    val ann = clazz.getAnnotation(classOf[SchemaReport])
    val fields = extractFieldDocs(clazz)
    ViewDoc(ann.value(), clazz.getSimpleName, ann.description(), fields)
  }

  private def extractFieldDocs(clazz: Class[_]): Seq[FieldDoc] = {
    // Use Java reflection for annotations (runtime-retained Java annotations)
    val ctor = clazz.getDeclaredConstructors.head
    val paramAnnotations = ctor.getParameterAnnotations

    // Use Scala reflection for accurate type info (avoids primitive erasure)
    val classSymbol = mirror.classSymbol(clazz)
    val ctorSymbol = classSymbol.primaryConstructor.asMethod
    val ctorParams = ctorSymbol.paramLists.flatten

    ctorParams.zipWithIndex.map { case (param, i) =>
      val annotations = paramAnnotations(i)
      val schemaDoc = annotations.collectFirst { case a: SchemaDoc => a }
      val description = schemaDoc.map(_.value()).getOrElse("")
      val unit = schemaDoc.map(_.unit()).getOrElse("")

      val scalaType = param.typeSignature
      val scalaTypeName = scalaTypeToName(scalaType)
      val sqlType = scalaTypeToSql(scalaType)

      // Extract nested fields for case class references in collections
      val innerClass = extractInnerCaseClass(scalaType)
      val nestedFields = innerClass match {
        case Some(c) if c != clazz => extractFieldDocs(c)
        case _ => Seq.empty
      }

      FieldDoc(
        name = param.name.toString,
        scalaType = scalaTypeName,
        sqlType = sqlType,
        unit = unit,
        description = description,
        nestedFields = nestedFields
      )
    }
  }

  /** Convert a Scala reflect Type to a human-readable Scala type name. */
  private def scalaTypeToName(t: Type): String = {
    val dealiased = t.dealias
    dealiased match {
      case TypeRef(_, sym, args) if args.nonEmpty =>
        s"${sym.name}[${args.map(scalaTypeToName).mkString(", ")}]"
      case TypeRef(_, sym, Nil) =>
        sym.name.toString
      case _ =>
        dealiased.typeSymbol.name.toString
    }
  }

  /** Convert a Scala reflect Type to a Spark SQL type string. */
  private def scalaTypeToSql(t: Type): String = {
    val dealiased = t.dealias
    val typeName = dealiased.typeSymbol.name.toString

    if (primitiveTypeMap.contains(typeName)) {
      primitiveTypeMap(typeName)
    } else {
      dealiased match {
        case TypeRef(_, sym, List(inner)) if sym.fullName == "scala.Option" =>
          scalaTypeToSql(inner)

        case TypeRef(_, sym, List(inner))
          if sym.fullName.startsWith("scala.collection") &&
             (sym.name.toString == "Seq" || sym.name.toString == "List") =>
          val innerTypeName = inner.typeSymbol.name.toString
          if (isCaseClassType(inner)) {
            val innerClass = mirror.runtimeClass(inner)
            val nestedFields = extractFieldDocs(innerClass)
            val structFields = nestedFields.map(f => s"${f.name}: ${f.sqlType}").mkString(", ")
            s"ARRAY<STRUCT<$structFields>>"
          } else {
            s"ARRAY<${scalaTypeToSql(inner)}>"
          }

        case TypeRef(_, sym, List(keyType, valType))
          if sym.fullName.startsWith("scala.collection") &&
             sym.name.toString == "Map" =>
          s"MAP<${scalaTypeToSql(keyType)}, ${scalaTypeToSql(valType)}>"

        case _ if isCaseClassType(dealiased) =>
          val innerClass = mirror.runtimeClass(dealiased)
          val nestedFields = extractFieldDocs(innerClass)
          val structFields = nestedFields.map(f => s"${f.name}: ${f.sqlType}").mkString(", ")
          s"STRUCT<$structFields>"

        case _ =>
          typeName.toUpperCase
      }
    }
  }

  private def isCaseClassType(t: Type): Boolean =
    t.typeSymbol.isClass && t.typeSymbol.asClass.isCaseClass

  /** Extract the inner case class from a Scala type (e.g. Seq[SqlNode] -> SqlNode). */
  private def extractInnerCaseClass(t: Type): Option[Class[_]] = {
    val dealiased = t.dealias
    dealiased match {
      case TypeRef(_, sym, List(inner))
        if sym.fullName.startsWith("scala.collection") &&
           (sym.name.toString == "Seq" || sym.name.toString == "List") =>
        if (isCaseClassType(inner)) Some(mirror.runtimeClass(inner)) else None

      case TypeRef(_, sym, List(inner)) if sym.fullName == "scala.Option" =>
        extractInnerCaseClass(inner)

      case _ if isCaseClassType(dealiased) =>
        val clazz = mirror.runtimeClass(dealiased)
        Some(clazz)

      case _ => None
    }
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
