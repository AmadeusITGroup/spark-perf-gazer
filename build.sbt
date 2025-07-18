import sbt._
import sbt.Keys._
import sbtrelease.ReleaseStateTransformations._
import sbtrelease.{versionFormatError, Version}

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
    if (isSnapshot.value)
      Some("snapshots" at artifactory + "sbt-built/")
    else
      Some("releases" at artifactory + "mvn-production/")
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
    coverageFailOnMinimum := false, // deal between mauricio and bruno
    coverageMinimumStmtTotal := 99.0,
    coverageMinimumBranchTotal := 99.0
  )
