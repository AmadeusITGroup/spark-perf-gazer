import Dependencies.deltaDepsBySparkVersion
import SparkCross.ProjectMatrixOps
import SparkCross.SparkAxis.{Spark341, Spark352}
import sbt.Keys.*
import sbt.Tests.*
import sbt.Compile

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
    Developer("b-joubert", "Bruno JOUBERT", "bruno.joubert@gmail.com", url("https://www.linkedin.com/in/bruno-joubert-0294415")),
    Developer("taccart", "Thierry ACCART", "thierry.accart@amadeus.com", url("https://www.linkedin.com/in/taccart/"))
    // To be completed by the other contributors via PR :)
  ),
  versionScheme := Some("semver-spec")
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
  libraryDependencies ++= Dependencies.coreDeps
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
  libraryDependencies ++= Dependencies.testDeps
)

lazy val core = (projectMatrix in file("core"))
  .settings(
    name := "perfgazer",
    commonSettings,
    testSettings,
    coverageFailOnMinimum := false,
    coverageMinimumStmtTotal := 95.0,
    coverageMinimumBranchTotal := 95.0
  )
  .addSparkVersionRow(spark = Spark341, scalaVersions = Seq("2.12.17"), customSetup = {
    _.settings(
      libraryDependencies ++= testDependencies(Spark341.sparkVersion)
    )
  })
  .addSparkVersionRow(spark = Spark352, scalaVersions = Seq("2.12.17", "2.13.16"), customSetup = {
    _.settings(
      libraryDependencies ++= testDependencies(Spark352.sparkVersion)
    )
  })

lazy val root = (project in file("."))
  // Aggregate all subprojects (like core) so their tasks are triggered from the root.
  .aggregate(core.projectRefs: _*)
  .settings(
    name := "perfgazer-root",
    publish / skip := true, // Do not publish artifacts from the root project (empty jars anyway)
  )
