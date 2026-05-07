import Dependencies.deltaDepsBySparkVersion
import sbt.Keys.*
import sbt.Tests.*
import sbt.Compile

val SparkVersionKey = "spark.perfgazer.sparkVersion"
val ScalaVersionKey = "spark.perfgazer.scalaVersion"
val DefaultSparkVersion = "3.5.2"
val DefaultScalaVersion = "2.12.20"
val SparkVersion = sys.props.getOrElse(SparkVersionKey, DefaultSparkVersion)
val BuildScalaVersion = sys.props.getOrElse(ScalaVersionKey, DefaultScalaVersion)

def sparkIdSuffix(version: String): String =
  "spark_" + version.replace('.', '-')

// Publishing settings
inThisBuild(List(
  organization := "io.github.amadeusitgroup",
  homepage := Some(url("https://github.com/AmadeusITGroup/spark-perf-gazer")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer("mauriciojost", "Mauricio JOST", "mauricio.jost@amadeus.com", url("https://www.linkedin.com/in/mauriciojost")),
    Developer("generoso", "Generoso PAGANO", "generoso.pagano@amadeus.com", url("https://www.linkedin.com/in/generoso-pagano-b4244230/")),
    Developer("b-joubert", "Bruno JOUBERT", "bruno.joubert@gmail.com", url("https://www.linkedin.com/in/bruno-joubert-0294415")),
    Developer("taccart", "Thierry ACCART", "thierry.accart@amadeus.com", url("https://www.linkedin.com/in/taccart/"))
    // To be completed by the other contributors via PR :)
  ),
  versionScheme := Some("semver-spec"),
  scalaVersion := BuildScalaVersion,
  crossScalaVersions := Seq(BuildScalaVersion)
))

val DefaultForkJavaOptions = Seq(
  "-Dspark.driver.bindAddress=127.0.0.1",
  "-Duser.country.format=US",
  "-Duser.language.format=en",
  "-Duser.timezone=UTC",
  "-Xms2000M",
  "-Xmx4000M",
  "-XX:+UseCompressedOops",
  "-XX:+UseG1GC"
)

def testDependencies(sparkVersion: String): Seq[ModuleID] = {
  Seq(
    "org.apache.spark"   %% "spark-core"   % sparkVersion  % Test,
    "org.apache.spark"   %% "spark-sql"    % sparkVersion  % Test,
    // Needed for perfgazer tests writing to delta
    deltaDepsBySparkVersion.getOrElse(
      sparkVersion,
      throw new IllegalArgumentException(s"Missing delta dependency for spark version $sparkVersion")
    ) % Test
  )
}

// Given tests, define test groups where each will have its own forked JVM for execution
// Currently there is one test group per *Spec.scala file
// Given that each test group has a different JVM, the SparkSession is not shared
def testGroups(tests: Seq[TestDefinition], baseDir: File): Seq[Group] = {
  val exportOptions = {
    val javaVersion = sys.props("java.specification.version").toDouble
    if (javaVersion >= 11) {
      Seq(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"
      )
    } else {
      Seq.empty
    }
  }
  tests
    .groupBy(t => t.name)
    .map { case (group, tests) =>
      val options = ForkOptions()
        .withWorkingDirectory(baseDir)
        .withRunJVMOptions(
          Vector(
            s"-Dtest.group=$group",
            s"-Dtest.basedir=$baseDir",
          ) ++ DefaultForkJavaOptions ++ exportOptions
        )
      new Group(group, tests, SubProcess(options))
    }.toSeq
}

val commonSettings = Seq(
  Compile / javacOptions ++= Seq("-source", "1.8", "-target", "1.8", "-parameters"),
  scalacOptions ++= {
    val base = Seq(
      "-deprecation",
      "-feature",
      "-encoding",
      "UTF-8"
    )
    val versionSpecific = CrossVersion.partialVersion(scalaVersion.value) match {
      case Some((2, 12)) => Seq("-Ypartial-unification", "-target:jvm-1.8")
      case Some((2, 13)) => Seq("-release:8")
      case _             => Seq.empty
    }
    base ++ versionSpecific
  },
  libraryDependencies ++= Dependencies.coreDeps(SparkVersion)
)

val testSettings = Seq(
  Test / scalacOptions ++= Seq("-Yrangepos"),
  Test / testGrouping := testGroups((Test / definedTests).value, (Test / run / baseDirectory).value),
  Test / parallelExecution := false,
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "-Duser.country.format=US",
    "-Duser.language.format=en",
    "-Xms512M",
    "-Xmx1G"
  ),
  Test / testOptions += Tests.Argument(TestFrameworks.JUnit, "-v", "-a"),
  libraryDependencies ++= Dependencies.testDeps ++ testDependencies(SparkVersion)
)

lazy val core = (project in file("core"))
  .settings(
    name := s"perfgazer_${sparkIdSuffix(SparkVersion)}",
    commonSettings,
    testSettings,
    coverageFailOnMinimum := false,
    coverageMinimumStmtTotal := 95.0,
    coverageMinimumBranchTotal := 95.0
  )

lazy val docGenerator = (project in file("doc-generator"))
  .dependsOn(core)
  .settings(
    name := "perfgazer-doc-generator",
    publish / skip := true,
    libraryDependencies ++= Seq(
      "org.apache.spark"  %% "spark-core"     % SparkVersion,
      "org.apache.spark"  %% "spark-sql"      % SparkVersion,
      "org.scala-lang"     % "scala-reflect"  % BuildScalaVersion,
      "org.scalatest"     %% "scalatest"      % "3.2.16"          % Test
    ),
    Compile / mainClass := Some("com.amadeus.perfgazer.docgen.SchemaDocGenerator")
  )

lazy val root = (project in file("."))
  // Aggregate all subprojects (like core) so their tasks are triggered from the root.
  .aggregate(core)
  .settings(
    name := "perfgazer-root",
    publish / skip := true, // Do not publish artifacts from the root project (empty jars anyway)
  )

addCommandAlias("generateDocs", "docGenerator/run")
