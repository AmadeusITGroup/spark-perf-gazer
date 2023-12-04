import sbt._

object Dependencies {

  def compileIfLocalOtherwiseProvided(): Configuration = {
    if (Option(System.getProperty("local")).mkString.equalsIgnoreCase("true")) {
      Compile
    } else {
      Provided
    }
  }

  val sparkVersion = "3.4.1"
  val coreDeps = Seq(
    "org.apache.spark"           %% "spark-core"    % sparkVersion % compileIfLocalOtherwiseProvided,
    "org.apache.spark"           %% "spark-sql"     % sparkVersion % compileIfLocalOtherwiseProvided,
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2" % Compile
  )

  val testDeps = Seq(
    "org.slf4j"           % "slf4j-log4j12" % "1.7.16" % Test,
    "org.apache.spark"   %% "spark-core"    % sparkVersion  % Test classifier "tests",
    "org.apache.spark"   %% "spark-sql"     % sparkVersion  % Test classifier "tests",
    "org.scalatest"      %% "scalatest"     % "3.2.10" % Test,
    "com.jayway.jsonpath" % "json-path"     % "2.8.0"  % Test,
    "io.delta"           %% "delta-core"    % "2.4.0"  % Test
  )
}
