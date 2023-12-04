package com.amadeus.testfwk

import com.typesafe.scalalogging.Logger
import org.slf4j.LoggerFactory

import java.nio.file.{Files, Path}
import scala.reflect.io.Directory

trait TempDirSupport {

  lazy val tmpDirLogger: Logger = Logger(LoggerFactory.getLogger(getClass.getName))
  /**
    * Method to be used in unit tests for automatic creation and cleaning of a temporary directory.
    */
  def withTmpDir[T](testCode: Path => T, cleanupEnabled: Boolean = true): T = {
    val tmpDir = Files.createTempDirectory("scala-test")
    try {
      testCode(tmpDir)
    } finally {
      if (cleanupEnabled) {
        new Directory(tmpDir.toFile).deleteRecursively()
      } else {
        tmpDirLogger.warn(s"Directory ${tmpDir} not being cleaned up (cleanupEnabled=false)")
      }
    }
  }

  def withTmpDirUncleaned[T](testCode: Path => T): T =
    withTmpDir(testCode = testCode, cleanupEnabled = false)
}

