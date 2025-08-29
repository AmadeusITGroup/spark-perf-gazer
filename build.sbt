import sbt._
import sbt.Keys._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.{versionFormatError, Version}

val DefaultForkJavaOptions = Seq(
  "-Dspark.driver.bindAddress=127.0.0.1",
  "-Duser.country.format=US",
  "-Duser.language.format=en",
  "-Duser.timezone=UTC",
  "-Xms2000M",
  "-Xmx4000M",
  "-XX:+CMSClassUnloadingEnabled",
  "-XX:+UseCompressedOops",
  "-XX:+UseG1GC"
)


import sbt.Tests._
// Given tests, define test groups where each will have its own forked JVM for execution
// Currently there is one test group per *Spec.scala file
// Given that each test group has a different JVM, the SparkSession is not shared
def testGroups(tests: Seq[TestDefinition], baseDir: File): Seq[Group] = {
  val onlyDisplayConfluencePages = System.getProperty("onlyDisplayConfluencePages")
  tests
    .groupBy(t => t.name)
    .map { case (group, tests) =>
      val options = ForkOptions()
        .withWorkingDirectory(baseDir)
        .withRunJVMOptions(
          Vector(
            s"-Dtest.group=${group}",
            s"-Dtest.basedir=${baseDir}",
            s"-DonlyDisplayConfluencePages=${onlyDisplayConfluencePages}"
          ) ++ DefaultForkJavaOptions
        )
      new Group(group, tests, SubProcess(options))
    } toSeq
}

/* Solve
[error] (assembly) deduplicate: different file contents found in the following:
[error] C:\...\https\repository.rnd.amadeus.net\sbt-sonatype-remote\com\fasterxml\jackson\core\jackson-annotations\2.12.7\jackson-annotations-2.12.7.jar:module-info.class
[error] C:\...\https\repository.rnd.amadeus.net\sbt-sonatype-remote\com\fasterxml\jackson\core\jackson-core\2.12.7\jackson-core-2.12.7.jar:module-info.class
[error] C:\...\https\repository.rnd.amadeus.net\sbt-sonatype-remote\com\fasterxml\jackson\core\jackson-databind\2.12.7\jackson-databind-2.12.7.jar:module-info.class
 */
assembly / assemblyMergeStrategy := {
    case PathList("module-info.class") => MergeStrategy.discard
    case PathList("META-INF", "MANIFEST.MF") => MergeStrategy.discard
    case x => MergeStrategy.first
}

val commonSettings = Seq(
  organization := "com.amadeus",
  scalaVersion := "2.12.13",
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
  Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oD", "-u", "target/test-reports"),
  libraryDependencies ++= Dependencies.testDeps
)

val publishSettings = Seq(
  publishTo := {
    val artifactory = "https://repository.rnd.amadeus.net/"
    if (isSnapshot.value) {
      Some("snapshots" at artifactory + "sbt-built/")
    } else {
      Some("releases" at artifactory + "mvn-production/")
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
          case Version.Bump.Bugfix => v.withoutQualifier.string // Already bumped by previous release
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

lazy val root = (project in file("."))
  .settings(
    name := "sparklear",
    commonSettings,
    releaseSettings,
    testSettings,
    publishSettings,
    coverageFailOnMinimum := true,
    coverageMinimumStmtTotal := 95.0,
    coverageMinimumBranchTotal := 95.0
  )
