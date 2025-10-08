import sbt.*
import sbt.Keys.*
import sbt.internal.ProjectMatrix

object DbrCross {

  /**
   * New projectmatrix axis representing a Databricks Runtime version.
   *
   * This axis is a WeakAxis, because you might depend in your project on submodules
   * which do not depend on a Databricks Runtime (e.g. non spark-related Scala library).
   *
   *
   */
  trait DbrAxis extends sbt.VirtualAxis.WeakAxis {

    /**
     * The precise scala version used in the Databricks Runtime.
     */
    def scalaVersion: String

    /**
     * The precise Spark version used in the Databricks Runtime.
     */
    def sparkVersion: String

    /**
     * The precise Hadoop version used in the Databricks Runtime.
     */
    def hadoopVersion: String

    /**
     * A minimum sets of dependencies which are provided by the DBR runtime,
     * and automatically added as dependencies to the rows of the project matrix.
     *
     * Because these are aimed at being added by default to all rows, they are kept minimal,
     * and do not included all the libraries provided by the Databricks Runtime.
     */
    def providedDeps: Seq[sbt.ModuleID]

    /**
     * This will be used for example to define the source directories which will be specific to one axis value (so one DBR)
     */
    override def directorySuffix: String

    /**
     * Behind the scenes, sbt-projectmatrix is creating one submodule for each projectmatrix row you create
     * (i.e. each combination of runtime and scala version that you provide in your build).
     * This idSuffix is used to distinguish these created submodules.
     */
    override def idSuffix: String
  }

  object DbrAxis {
    case object Dbr13_3 extends DbrAxis {
      override val scalaVersion: String = "2.12.15"
      override val sparkVersion: String = "3.4.1"
      override val hadoopVersion: String = "3.3.1"

      override val providedDeps: Seq[sbt.ModuleID] = Seq(
        "org.apache.spark"   %% "spark-core"   % sparkVersion  % Dependencies.compileIfLocalOtherwiseProvided,
        "org.apache.spark"   %% "spark-sql"    % sparkVersion  % Dependencies.compileIfLocalOtherwiseProvided
      )

      override val directorySuffix: String = "dbr13.3"
      override val idSuffix: String = "Dbr13"
    }

    case object Dbr16_4 extends DbrAxis {
      override val scalaVersion: String = "2.12.17"
      override val sparkVersion: String = "3.5.2"
      override val hadoopVersion: String = "3.3.2"

      override val providedDeps: Seq[sbt.ModuleID] = Seq(
        "org.apache.spark"   %% "spark-core"   % sparkVersion  % Dependencies.compileIfLocalOtherwiseProvided,
        "org.apache.spark"   %% "spark-sql"    % sparkVersion  % Dependencies.compileIfLocalOtherwiseProvided
      )

      override val directorySuffix: String = "dbr16.4"
      override val idSuffix: String = "Dbr16"
    }
  }

  implicit class ProjectMatrixOps(val project: ProjectMatrix) extends AnyVal {

    /**
     * Add a row to the project matrix for a Databricks Runtime version.
     * @param dbr the [DbrAxis] value corresponding to the Databricks Runtime version
     * @param customSetup a function to apply custom settings to the project
     *                    (for example, to add custom library dependencies which relies on Spark)
     */
    def addDatabricksRuntimeRow(dbr: DbrAxis, customSetup: Project => Project): ProjectMatrix = {
      project.customRow(
        scalaVersions = Seq(dbr.scalaVersion),
        axisValues = Seq(dbr, VirtualAxis.jvm),
        process = { p: Project =>
          p.settings(
            Seq(
              name ~= (_ + "_" + dbr.idSuffix),
              libraryDependencies ++= dbr.providedDeps
            )
          )
        }.andThen(customSetup)
      )
    }
  }
}