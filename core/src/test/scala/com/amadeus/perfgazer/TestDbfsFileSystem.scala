package com.amadeus.perfgazer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.fs.permission.FsPermission
import org.apache.hadoop.util.Progressable

import java.io.{File, FileNotFoundException}
import java.net.URI

/**
 * A test-only FileSystem implementation that handles the `dbfs:/` scheme
 * by delegating to the local filesystem. This allows integration tests
 * to exercise HDFS mode code paths without requiring a real distributed
 * filesystem.
 *
 * The `dbfs:/path` URI is mapped to the local path `/path`.
 */
class TestDbfsFileSystem extends FileSystem {

  private var localFs: FileSystem = _
  private var myUri: URI = URI.create("dbfs:///")

  override def getScheme: String = "dbfs"

  override def getUri: URI = myUri

  override def initialize(name: URI, conf: Configuration): Unit = {
    super.initialize(name, conf)
    myUri = URI.create("dbfs:///")
    localFs = FileSystem.getLocal(conf)
  }

  private def toLocalPath(path: Path): Path = {
    val uri = path.toUri
    new Path(uri.getPath)
  }

  override def open(f: Path, bufferSize: Int): org.apache.hadoop.fs.FSDataInputStream = {
    localFs.open(toLocalPath(f), bufferSize)
  }

  override def create(
    f: Path,
    permission: FsPermission,
    overwrite: Boolean,
    bufferSize: Int,
    replication: Short,
    blockSize: Long,
    progress: Progressable
  ): org.apache.hadoop.fs.FSDataOutputStream = {
    localFs.create(toLocalPath(f), permission, overwrite, bufferSize, replication, blockSize, progress)
  }

  override def append(f: Path, bufferSize: Int, progress: Progressable): org.apache.hadoop.fs.FSDataOutputStream = {
    localFs.append(toLocalPath(f), bufferSize, progress)
  }

  override def rename(src: Path, dst: Path): Boolean = {
    localFs.rename(toLocalPath(src), toLocalPath(dst))
  }

  override def delete(f: Path, recursive: Boolean): Boolean = {
    localFs.delete(toLocalPath(f), recursive)
  }

  override def listStatus(f: Path): Array[FileStatus] = {
    localFs.listStatus(toLocalPath(f))
  }

  override def setWorkingDirectory(newDir: Path): Unit = {
    localFs.setWorkingDirectory(toLocalPath(newDir))
  }

  override def getWorkingDirectory: Path = {
    localFs.getWorkingDirectory
  }

  override def mkdirs(f: Path, permission: FsPermission): Boolean = {
    localFs.mkdirs(toLocalPath(f), permission)
  }

  override def getFileStatus(f: Path): FileStatus = {
    localFs.getFileStatus(toLocalPath(f))
  }

  override def exists(f: Path): Boolean = {
    localFs.exists(toLocalPath(f))
  }

  override def copyFromLocalFile(delSrc: Boolean, overwrite: Boolean, src: Path, dst: Path): Unit = {
    localFs.copyFromLocalFile(delSrc, overwrite, src, toLocalPath(dst))
  }
}
