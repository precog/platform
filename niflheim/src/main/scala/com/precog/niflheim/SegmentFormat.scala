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
