package com.precog.niflheim

import com.precog.util.PrecogUnit

import java.io.{ File, IOException }
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }

import scalaz.Validation

trait SegmentWriter {
  def writeSegment(channel: WritableByteChannel, segment: Segment): Validation[IOException, PrecogUnit]
}

trait SegmentReader {
  def readSegmentId(channel: ReadableByteChannel): Validation[IOException, SegmentId]
  def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment]
}

trait SegmentFormat {
  def reader: SegmentReader
  def writer: SegmentWriter
}
