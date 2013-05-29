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
package com.precog.niflheim

import com.precog.common._
import com.precog.util._

import java.io._
import java.nio.channels.WritableByteChannel

import akka.actor._

import scalaz._
import scalaz.syntax.traverse._
import scalaz.std.list._

case class Prepare(blockid: Long, seqId: Long, root: File, source: StorageReader)
case class Spoilt(blockid: Long, seqId: Long)
case class Cooked(blockid: Long, seqId: Long, root: File, metadata: File)

final case class Chef(blockFormat: CookedBlockFormat, format: SegmentFormat) extends Actor {
  private def typeCode(ctype: CType): String = CType.nameOf(ctype)

  def prefix(id: Segment): String = {
    val pathHash = id.cpath.hashCode.toString
    "segment-" + id.blockid + "-" + pathHash + "-" + typeCode(id.ctype)
  }

  def cook(root: File, reader: StorageReader): ValidationNel[IOException, File] = {
    assert(root.exists)
    assert(root.isDirectory)
    assert(root.canWrite)
    val files0 = reader.snapshot(None).segments map { seg =>
      val file = File.createTempFile(prefix(seg), ".cooked", root)
      val relativized = new File(file.getName)
      val channel: WritableByteChannel = new FileOutputStream(file).getChannel()
      val result = try {
        format.writer.writeSegment(channel, seg) map { _ => (seg.id, relativized) }
      } finally {
        channel.close()
      }
      result.toValidationNel
    }

    val files = files0.toList.sequence[({ type λ[α] = ValidationNel[IOException, α] })#λ, (SegmentId, File)]
    files flatMap { segs =>
      val metadata = CookedBlockMetadata(reader.id, reader.length, segs.toArray)
      val mdFile = File.createTempFile("block-%08x".format(reader.id), ".cookedmeta", root)
      val channel = new FileOutputStream(mdFile).getChannel()
      try {
        blockFormat.writeCookedBlock(channel, metadata).toValidationNel.map { _ : PrecogUnit =>
          new File(mdFile.getName)
        }
      } finally {
        channel.close()
      }
    }
  }

  def receive = {
    case Prepare(blockid, seqId, root, source) =>
      cook(root, source) match {
        case Success(file) =>
          sender ! Cooked(blockid, seqId, root, file)
        case Failure(_) =>
          sender ! Spoilt(blockid, seqId)
      }
  }
}

