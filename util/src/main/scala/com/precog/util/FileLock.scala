/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
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

      lockFile.foreach(_.delete)
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

    LockHolder(channel, lock, if (removeFile) Some(lockFile) else None)
  }
}
