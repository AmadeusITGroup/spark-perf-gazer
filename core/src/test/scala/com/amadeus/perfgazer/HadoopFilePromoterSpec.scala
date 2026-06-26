package com.amadeus.perfgazer

import com.amadeus.testfwk.SimpleSpec
import org.apache.hadoop.conf.Configuration

import java.io.File
import java.nio.file.Files

class HadoopFilePromoterSpec extends SimpleSpec {

  describe("HadoopFilePromoter initialization") {

    it("should throw IllegalStateException when FileSystem cannot be obtained") {
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.nonexistent.impl", "com.does.not.Exist")

      val promoter = new HadoopFilePromoter("nonexistent://invalid-bucket/path/", hadoopConf)
      // Lazy init triggers on first promote() call
      val dummyFile = Files.createTempFile("dummy-", ".json").toFile
      try {
        val ex = intercept[IllegalStateException] {
          promoter.promote(dummyFile)
        }
        ex.getMessage should include("Failed to obtain Hadoop FileSystem")
      } finally {
        dummyFile.delete()
      }
    }

    it("should create remote destination directory if it does not exist") {
      val baseDir = Files.createTempDirectory("promoter-init-mkdirs-")
      val targetDir = new File(baseDir.toFile, "subdir/nested")
      try {
        targetDir.exists() shouldBe false

        val hadoopConf = new Configuration()
        hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

        val promoter = new HadoopFilePromoter(targetDir.toURI.toString, hadoopConf)

        // Lazy init triggers on first promote() — create a dummy file to promote
        val localFile = Files.createTempFile("dummy-promote-", ".json").toFile
        Files.write(localFile.toPath, "test".getBytes)
        promoter.promote(localFile)

        targetDir.exists() shouldBe true
        targetDir.isDirectory shouldBe true
      } finally {
        deleteRecursively(baseDir.toFile)
      }
    }

    it("should fail fast on init() when FileSystem cannot be obtained") {
      val hadoopConf = new Configuration()
      hadoopConf.set("fs.nonexistent.impl", "com.does.not.Exist")

      val promoter = new HadoopFilePromoter("nonexistent://invalid-bucket/path/", hadoopConf)

      val ex = intercept[IllegalStateException] {
        promoter.init()
      }
      ex.getMessage should include("Failed to obtain Hadoop FileSystem")
    }

    it("should create remote destination directory eagerly on init()") {
      val baseDir = Files.createTempDirectory("promoter-init-eager-")
      val targetDir = new File(baseDir.toFile, "subdir/nested")
      try {
        targetDir.exists() shouldBe false

        val hadoopConf = new Configuration()
        hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

        val promoter = new HadoopFilePromoter(targetDir.toURI.toString, hadoopConf)
        promoter.init()

        targetDir.exists() shouldBe true
        targetDir.isDirectory shouldBe true
      } finally {
        deleteRecursively(baseDir.toFile)
      }
    }
  }

  describe("HadoopFilePromoter.promote") {

    it("should copy file to remote destination with preserved name and delete local file") {
      val localDir = Files.createTempDirectory("promoter-local-")
      val remoteDir = Files.createTempDirectory("promoter-remote-")

      try {
        val localFile = new File(localDir.toFile, "sql-reports-abc123.json")
        Files.write(localFile.toPath, "test-content".getBytes)

        val hadoopConf = new Configuration()
        hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

        val promoter = new HadoopFilePromoter(remoteDir.toUri.toString, hadoopConf)
        promoter.promote(localFile)

        new File(remoteDir.toFile, "sql-reports-abc123.json") should exist
        localFile should not(exist)
      } finally {
        deleteRecursively(localDir.toFile)
        deleteRecursively(remoteDir.toFile)
      }
    }

    it("should retain local file when copy operation fails") {
      val localDir = Files.createTempDirectory("promoter-local-fail-")
      val remoteDir = Files.createTempDirectory("promoter-remote-fail-")

      try {
        val localFile = new File(localDir.toFile, "sql-reports-abc123.json")
        Files.write(localFile.toPath, "test-content".getBytes)

        remoteDir.toFile.setWritable(false)

        val hadoopConf = new Configuration()
        hadoopConf.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

        val promoter = new HadoopFilePromoter(remoteDir.toUri.toString, hadoopConf)
        promoter.promote(localFile)

        localFile should exist
      } finally {
        remoteDir.toFile.setWritable(true)
        deleteRecursively(localDir.toFile)
        deleteRecursively(remoteDir.toFile)
      }
    }
  }

  private def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles().foreach(deleteRecursively)
    }
    file.delete()
  }
}
