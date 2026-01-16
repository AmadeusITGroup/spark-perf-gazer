import Dependencies.deltaDepsBySparkVersion
import SparkCross.ProjectMatrixOps
import SparkCross.SparkAxis.{Spark341, Spark352}
import sbt.Keys.*
import sbt.Tests.*
import sbt.Compile

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
  versionScheme := Some("semver-spec")
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
  Compile / javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq(
    "-Ypartial-unification",
    "-deprecation",
    "-feature",
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8"
  ),
  libraryDependencies ++= Dependencies.coreDeps
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
  .addSparkVersionRow(spark = Spark352, scalaVersions = Seq("2.12.17"), customSetup = {
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
