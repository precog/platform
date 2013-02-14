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
import com.precog.util.PrecogUnit

import java.io.{ IOException, File }
import java.nio.ByteBuffer
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }

import scalaz._

trait CookedBlockFormat {
  def readCookedBlock(channel: ReadableByteChannel): Validation[IOException, Array[(SegmentId, File)]]
  def writeCookedBlock(channel: WritableByteChannel, segments: Array[(SegmentId, File)]): Validation[IOException, PrecogUnit]
}

object V1CookedBlockFormat extends CookedBlockFormat with Chunker {
  val verify = true

  val FileCodec = Codec.Utf8Codec.as[File](_.getCanonicalPath(), new File(_))
  val CPathCodec = Codec.Utf8Codec.as[CPath](_.toString, CPath(_))
  val CTypeCodec = Codec.ArrayCodec(Codec.ByteCodec).as[CType](CTypeFlags.getFlagFor, CTypeFlags.cTypeForFlag)
  val ColumnRefCodec = Codec.CompositeCodec[CPath, CType, (CPath, CType)](CPathCodec, CTypeCodec,
    identity, { (a: CPath, b: CType) => (a, b) })

  val SegmentIdCodec = Codec.CompositeCodec[Long, (CPath, CType), SegmentId](
    Codec.LongCodec, ColumnRefCodec,
    { id: SegmentId => (id.blockid, (id.cpath, id.ctype)) },
    { case (blockid, (cpath, ctype)) => SegmentId(blockid, cpath, ctype) })

  val SegmentsCodec = Codec.ArrayCodec({
    Codec.CompositeCodec[SegmentId, File, (SegmentId, File)](SegmentIdCodec, FileCodec, identity, _ -> _)
  })

  def writeCookedBlock(channel: WritableByteChannel, segments: Array[(SegmentId, File)]) = {
    write(channel, SegmentsCodec.maxSize(segments)) { buffer =>
      SegmentsCodec.writeUnsafe(segments, buffer)
      PrecogUnit
    }
  }

  def readCookedBlock(channel: ReadableByteChannel): Validation[IOException, Array[(SegmentId, File)]] = {
    read(channel) map (SegmentsCodec.read)
  }
}

case class VersionedCookedBlockFormat(formats: Map[Int, CookedBlockFormat]) extends CookedBlockFormat with Versioning {
  val magic: Short = 0xB10C.toShort
  val (version, format) = {
    val (ver, fmt) = formats.maxBy(_._1)
    (ver.toShort, fmt)
  }

  def writeCookedBlock(channel: WritableByteChannel, segments: Array[(SegmentId, File)]) = {
    for {
      _ <- writeVersion(channel)
      _ <- format.writeCookedBlock(channel, segments)
    } yield PrecogUnit
  }

  def readCookedBlock(channel: ReadableByteChannel): Validation[IOException, Array[(SegmentId, File)]] = {
    readVersion(channel) flatMap { version =>
      formats get version map { format =>
        format.readCookedBlock(channel)
      } getOrElse {
        Failure(new IOException(
          "Invalid version found. Expected one of %s, found %d." format (formats.keys mkString ",", version)))
      }
    }
  }
}
