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

  // @formatter:off
  val coreDeps = Seq(
    "org.apache.spark"           %% "spark-core"      % sparkVersion  % compileIfLocalOtherwiseProvided,
    "org.apache.spark"           %% "spark-sql"       % sparkVersion  % compileIfLocalOtherwiseProvided,
    "org.apache.parquet"          % "parquet-avro"    % "1.13.1"      % Compile,
    "org.apache.avro"             % "avro"            % "1.11.1"      % compileIfLocalOtherwiseProvided
  )

  val testDeps = Seq(
    "org.slf4j"                   % "slf4j-log4j12"   % "1.7.16"      % Test,
    "org.apache.spark"           %% "spark-core"      % sparkVersion  % Test classifier "tests",
    "org.apache.spark"           %% "spark-sql"       % sparkVersion  % Test classifier "tests",
    "org.scalatest"              %% "scalatest"       % "3.2.10"      % Test,
    "org.scalamock"              %% "scalamock"       % "5.1.0"       % Test,
    "com.jayway.jsonpath"         % "json-path"       % "2.8.0"       % Test,
    "io.delta"                   %% "delta-core"      % "2.4.0"       % Test
  )
  // @formatter:on

}
