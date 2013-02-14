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
import com.precog.common.json._

import java.lang.ref.SoftReference

import java.io.{ File, IOException }
import java.io.FileInputStream
import java.nio.channels._

import scalaz._
import scalaz.syntax.traverse._
import scalaz.std.list._

final class CookedReader(metadata: File, blockFormat: CookedBlockFormat, segmentFormat: SegmentFormat) {

  private val lock = new AnyRef { }

  @volatile
  private var block: SoftReference[List[(SegmentId, File)]] = null

  private def maybeBlock = if (block != null) block.get() else null

  private def read[A](file: File)(f: ReadableByteChannel => A): A = {
    val channel = new FileInputStream(file).getChannel()
    try {
      f(channel)
    } finally {
      channel.close()
    }
  }

  private def loadFromDisk(): Validation[IOException, List[(SegmentId, File)]] = {
    read(metadata) { channel =>
      val segsV = blockFormat.readCookedBlock(channel) map (_.toList)
      segsV foreach { segs0 =>
        block = new SoftReference(segs0)
      }
      segsV
    }
  }

  def segments: Validation[IOException, List[(SegmentId, File)]] = {
    val segs = maybeBlock
    if (segs != null) {
      Success(segs)
    } else {
      lock.synchronized {
        val block = maybeBlock
        if (block == null) {
          loadFromDisk()
        } else {
          Success(block)
        }
      }
    }
  }

  private def segmentsByCPath: Validation[IOException, Map[CPath, List[File]]] = segments map { segs0 =>
    segs0.groupBy(_._1.cpath).map { case (cpath, segs1) =>
      (cpath, segs1 map (_._2))
    }.toMap
  }

  def blockids: Validation[IOException, Set[Long]] = segments map { segs =>
    segs.map(_._1.blockid).toSet
  }

  def load(paths: List[CPath]): ValidationNEL[IOException, List[(CPath, List[Segment])]] = {
    segmentsByCPath.toValidationNEL flatMap { (segsByPath: Map[CPath, List[File]]) =>
      paths.map { path =>
        val v: ValidationNEL[IOException, List[Segment]] = segsByPath.getOrElse(path, Nil).map { file =>
          read(file) { channel =>
            segmentFormat.reader.readSegment(channel).toValidationNEL
          }
        }.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, Segment]
        v map (path -> _)
      }.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, (CPath, List[Segment])]
    }
  }
}
