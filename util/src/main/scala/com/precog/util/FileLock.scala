package com.precog.util

import java.io.{File, RandomAccessFile}
import java.nio.channels.{FileChannel, FileLock => JFileLock}

trait FileLock {
  def release: Unit
}

class FileLockException(message: String) extends Exception(message)

object FileLock {
  private case class LockHolder(channel: FileChannel, lock: JFileLock, lockFile: Option[File]) extends FileLock {
    def release = {
      lock.release
      channel.close

      if (removeFile) {
        lockFile.delete
      }
    }


  def apply(target: File, lockPrefix: String = "LOCKFILE"): FileLock = {
    val (lockFile, removeFile) = if (target.isDirectory) {
      val lockFile = new File(target, lockPrefix + ".lock")
      lockFile.createNewFile
      (lockFile, true)
    } else {
      (target, false)
    }

    val channel = new RandomAccessFile(lockFile, "rw").getChannel
    val lock = channel.tryLock

    if (lock == null) {
      throw new FileLockException("Could not lock. Previous lock exists on " + target)
    }

    new FileLock {
      def release = {
        lock.release
        channel.close

        if (removeFile) {
          lockFile.delete
        }
      }
    }
  }
}
