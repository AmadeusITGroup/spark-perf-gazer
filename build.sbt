import Dependencies.deltaDepsBySparkVersion
import sbt.{Compile, *}
import sbt.Keys.*
import sbt.Tests.*
import sbtrelease.ReleaseStateTransformations.*
import sbtrelease.{Version, versionFormatError}
import sbtrelease.ReleasePlugin.autoImport.*

//import DbrCross.DbrAxis.{Dbr13_3, Dbr16_4}
//import DbrCross.ProjectMatrixOps
import SparkCross.SparkAxis.{Spark341, Spark352}
import SparkCross.ProjectMatrixOps

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
    // Needed for sparklear tests writing to delta
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
    if (javaVersion >= 11)
      Seq(
        "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
        "--add-opens=java.base/java.lang=ALL-UNNAMED",
        "--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
        "--add-exports=java.base/sun.util.calendar=ALL-UNNAMED"
      )
    else
      Seq.empty
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
    } toSeq
}

val commonSettings = Seq(
  organization := "com.amadeus",
  update / checksums := Nil,
  Compile / javacOptions ++= Seq("-source", "1.8", "-target", "1.8"),
  scalacOptions ++= Seq(
    "-Ypartial-unification",
    "-deprecation",
    "-feature",
    "-encoding",
    "UTF-8",
    "-target:jvm-1.8"
  ),
  libraryDependencies ++= Dependencies.coreDeps,
  isSnapshot := version.value.contains("SNAPSHOT")
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

val publishSettings = Seq(
  publishTo := {
    val artifactory = "https://repository.rnd.amadeus.net/"
    if (isSnapshot.value) {
      Some("snapshots" at artifactory + "ssce-sbt-dev-ssce-nce/")
    } else {
      Some("releases" at artifactory + "ssce-sbt-release-ssce-nce/")
    }
  },
  Test / publishArtifact := true
)

val releaseSettings = Seq(
  releaseCommitMessage := s"[sbt-release] Setting version to ${(ThisBuild / version).value}",
  releaseNextCommitMessage := s"[sbt-release] Setting version to ${(ThisBuild / version).value}",
  releaseIgnoreUntrackedFiles := true,
  releaseVersion := { ver =>
    Version(ver)
      .map { v =>
        suggestedBump.value match {
          case Version.Bump.Bugfix => v.withoutQualifier.string
          case _ => v.bump(suggestedBump.value).withoutQualifier.string
        }
      }
      .getOrElse(versionFormatError(ver))
  },
  releaseProcess := Seq[ReleaseStep](
    checkSnapshotDependencies,
    inquireVersions,
    setReleaseVersion,
    commitReleaseVersion,
    tagRelease,
    pushChanges,
    publishArtifacts,
    setNextVersion,
    commitNextVersion,
    pushChanges
  ),
  bugfixRegexes := List("""\[bugfix\].*""", """\[fix\].*""", """\[technical\].*""").map(_.r),
  minorRegexes := List("""\[minor\].*""", """\[feature\].*""").map(_.r),
  majorRegexes := List("""\[major\].*""", """\[breaking\].*""").map(_.r)
)

lazy val core = (projectMatrix in file("core"))
  .settings(
    name := "sparklear",
    commonSettings,
    testSettings,
    publishSettings,
    coverageFailOnMinimum := false,
    coverageMinimumStmtTotal := 95.0,
    coverageMinimumBranchTotal := 95.0
  )
  /*
  .addDatabricksRuntimeRow(Dbr13_3, customSetup = {
    _.settings(
      libraryDependencies ++= sparkDependencies(Dbr13_3.sparkVersion)
    )
  })
  .addDatabricksRuntimeRow(Dbr16_4, customSetup = {
    _.settings(
      libraryDependencies ++= sparkDependencies(Dbr16_4.sparkVersion)
    )
  })
  */
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
    releaseSettings: _*
  )