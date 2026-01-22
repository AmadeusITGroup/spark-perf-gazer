import Dependencies.deltaDepsBySparkVersion
import sbt.Keys.*
import sbt.Tests.*
import sbt.Compile

val SparkVersionKey = "spark.perfgazer.sparkVersion"
val ScalaVersionKey = "spark.perfgazer.scalaVersion"
val DefaultSparkVersion = "3.5.2"
val DefaultScalaVersion = "2.12.17"
val SparkVersion = sys.props.getOrElse(SparkVersionKey, DefaultSparkVersion)
val BuildScalaVersion = sys.props.getOrElse(ScalaVersionKey, DefaultScalaVersion)

def sparkIdSuffix(version: String): String =
  "spark_" + version.replace('.', '-')

def testJvmExportOptions: Seq[String] = {
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

// Publishing settings
inThisBuild(List(
  organization := "io.github.amadeusitgroup",
  homepage := Some(url("https://github.com/AmadeusITGroup/spark-perf-gazer")),
  licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
  developers := List(
    Developer("mauriciojost", "Mauricio JOST", "mauricio.jost@amadeus.com", url("https://www.linkedin.com/in/mauriciojost")),
    Developer("generoso", "Generoso PAGANO", "generoso.pagano@amadeus.com", url("https://www.linkedin.com/in/generoso-pagano-b4244230/")),
    Developer("b-joubert", "Bruno JOUBERT", "bruno.joubert@gmail.com", url("https://www.linkedin.com/in/bruno-joubert-0294415"))
    // To be completed by the other contributors via PR :)
  ),
  versionScheme := Some("semver-spec"),
  scalaVersion := BuildScalaVersion,
  crossScalaVersions := Seq(BuildScalaVersion)
))

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

val commonSettings = Seq(
  Compile / javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq(
    "-Ypartial-unification",
    "-deprecation",
    "-feature",
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8"
  ),
  libraryDependencies ++= Dependencies.coreDeps(SparkVersion)
)

val testSettings = Seq(
  Test / scalacOptions ++= Seq("-Yrangepos"),
  Test / parallelExecution := false,
  Test / fork := true,
  Test / javaOptions ++= Seq(
    "-Dspark.driver.bindAddress=127.0.0.1",
    "-Duser.country.format=US",
    "-Duser.language.format=en",
    "-Duser.timezone=UTC",
    "-Xms512M",
    "-Xmx1G"
  ) ++ testJvmExportOptions,
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

lazy val root = (project in file("."))
  // Aggregate all subprojects (like core) so their tasks are triggered from the root.
  .aggregate(core)
  .settings(
    name := "perfgazer-root",
    publish / skip := true, // Do not publish artifacts from the root project (empty jars anyway)
  )
