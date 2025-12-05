package com.amadeus.perfgazer

import java.io.File
import scala.io.{Codec, Source}

object FileUtils {

  def files(dir: File): Seq[File] = {
    dir.listFiles().filter(file => file.isFile).toSeq
  }

  def readLines(files: Seq[File], codec: Codec = scala.io.Codec.UTF8): Seq[String] = {
    files.flatMap { file =>
      val source = Source.fromFile(file)(codec)
      try {
        source.getLines.toList
      } finally {
        source.close()
      }
    }
  }
}
