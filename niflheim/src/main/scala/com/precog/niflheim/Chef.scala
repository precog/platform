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
case class Cooked(blockid: Long, seqId: Long, root: File, files: Seq[(SegmentId, File)])

final case  class Chef(format: SegmentFormat) extends Actor {
  private def typeCode(ctype: CType): String = CType.nameOf(ctype)

  def prefix(id: Segment): String = {
    val pathHash = id.cpath.hashCode.toString
    id.blockid + "-" + pathHash + "-" + typeCode(id.ctype)
  }

  def cook(root: File, segments: List[Segment]): ValidationNEL[IOException, List[(SegmentId, File)]] = {
    val files = segments map { seg =>
      val file = File.createTempFile(prefix(seg), ".cooked", root)
      val channel: WritableByteChannel = new FileOutputStream(file).getChannel()
      val result = format.writer.writeSegment(channel, seg) map { _ => (seg.id, file) }
      channel.close()
      result.toValidationNEL
    }

    files.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, (SegmentId, File)]
  }

  def receive = {
    case Prepare(blockid, seqId, root, source) =>
      cook(root, source.snapshot.segments) match {
        case Success(files) =>
          sender ! Cooked(blockid, seqId, root, files)
        case Failure(_) =>
          sender ! Spoilt(blockid, seqId)
      }
  }
}

