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

import com.precog.util.PrecogUnit
import com.precog.common.json._

import java.io.{ File, IOException }
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }

import scalaz.Validation

trait SegmentWriter {
  def writeSegment(channel: WritableByteChannel, segment: Segment): Validation[IOException, PrecogUnit]
}

trait SegmentReader {
  //def readSegmentId(channel: ReadableByteChannel): Validation[IOException, SegmentId]
  def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment]
}

trait SegmentFormat {
  def reader: SegmentReader
  def writer: SegmentWriter
}

//trait SegmentContext {
//  def load[A](id: SegmentId)(f: ReadableByteChannel => Validation[IOException, A]): Validation[IOException, Option[A]]
//  def create[A](id: SegmentId)(f: WritableByteChannel => Validation[IOException, A]): Validation[IOException, A]
//}
//
//trait SegmentStore {
//  def context: SegmentContext
//  def format: SegmentFormat
//
//  def cook(segments: List[Segment]): Validation[IOException, List[File]]
//
//  def load(id: SegmentId): Validation[IOException, Option[Segment]] = {
//    context.load(id) (_ map { channel =>
//      format.reader.readSegment(channel)
//    })
//  }
//
//  def save(segment: Segment): Validation[IOException, SegmentId] = {
//    context.create(segment.id) { channel =>
//      format.writer.writeSegment(channel, segment) map (_ => segment.id)
//    }
//  }
//}
