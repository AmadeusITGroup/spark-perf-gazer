import sbt._

object Dependencies {

  def compileIfLocalOtherwiseProvided(): Configuration = {
    if (Option(System.getProperty("local")).mkString.equalsIgnoreCase("true")) {
      Compile
    } else {
      Provided
    }
  }

  val sparkCore      = "org.apache.spark"           %% "spark-core"                % "3.3.0"
  val sparkSql       = "org.apache.spark"           %% "spark-sql"                 % "3.3.0"
  val scalaLogging   = "com.typesafe.scala-logging" %% "scala-logging"             % "3.9.2"
  val slf4jLog4j     = "org.slf4j"                   % "slf4j-log4j12"             % "1.7.16"
  val scalaTest      = "org.scalatest"              %% "scalatest"                 % "3.2.10"
  val jsonpath       = "com.jayway.jsonpath"         % "json-path"                 % "2.8.0"

  val coreDeps = Seq(
    sparkCore      % compileIfLocalOtherwiseProvided,
    sparkSql       % compileIfLocalOtherwiseProvided,
    scalaLogging   % Compile
  )

  val testDeps = Seq(
    slf4jLog4j     % Test,
    sparkCore      % Test classifier "tests",
    sparkSql       % Test classifier "tests",
    scalaTest      % Test,
    jsonpath       % Test
  )
}
