import sbt._

object Dependencies {

  def compileIfLocalOtherwiseProvided(): Configuration = {
    if (Option(System.getProperty("local")).mkString.equalsIgnoreCase("true")) {
      Compile
    } else {
      Provided
    }
  }

  val deltaDepsBySparkVersion: Map[String, ModuleID] = Map(
    "3.4.1" -> "io.delta" %% "delta-core" % "2.4.0",
    "3.5.0" -> "io.delta" %% "delta-spark" % "3.2.0",
    "3.5.2" -> "io.delta" %% "delta-spark" % "3.2.0"
  )

  // @formatter:off
  def coreDeps(sparkVersion: String): Seq[ModuleID] = Seq(
    "org.apache.spark"           %% "spark-core"      % sparkVersion  % compileIfLocalOtherwiseProvided,
    "org.apache.spark"           %% "spark-sql"       % sparkVersion  % compileIfLocalOtherwiseProvided
  )

  val testDeps: Seq[ModuleID] = Seq(
    "org.slf4j"                   % "slf4j-log4j12"   % "1.7.16"      % Test,
    "org.scalatest"              %% "scalatest"       % "3.2.16"      % Test,
    "org.scalamock"              %% "scalamock"       % "5.1.0"       % Test,
    "com.jayway.jsonpath"         % "json-path"       % "2.8.0"       % Test,
    "com.github.sbt"              % "junit-interface" % "0.13.3"      % Test
  )
  // @formatter:on
}
