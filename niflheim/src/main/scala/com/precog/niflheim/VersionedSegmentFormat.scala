package com.precog.niflheim

import com.precog.util.PrecogUnit

import java.io.{ File, IOException }
import java.nio.channels.{ ReadableByteChannel, WritableByteChannel }
import java.nio.ByteBuffer

import scalaz.{ Validation, Success, Failure }

/**
 * A `VersionedSegmentFormat` wraps formats and is used to deal with multiple
 * versions for `SegmentFormat`s. The version of a segment format is always
 * written first, followed by the actual segment. The format with the highest
 * version number is always used for writing. For reads, the version is read
 * first and the format corresponding to this version is used to read the rest
 * of the segment. If no format exists for that version, then we return an
 * error.
 */
case class VersionedSegmentFormat(formats: Map[Int, SegmentFormat]) extends SegmentFormat with Versioning {
  val magic: Short = 0x0536.toShort
  val (version, format) = {
    val (ver, format) = formats.maxBy(_._1)
    (ver.toShort, format)
  }

  object writer extends SegmentWriter {
    def writeSegment(channel: WritableByteChannel, segment: Segment) = {
      for {
        _ <- writeVersion(channel)
        _ <- format.writer.writeSegment(channel, segment)
      } yield PrecogUnit
    }
  }

  object reader extends SegmentReader {
    def readSegmentId(channel: ReadableByteChannel): Validation[IOException, SegmentId] = {
      readVersion(channel) flatMap { version =>
        formats get version map { format =>
          format.reader.readSegmentId(channel)
        } getOrElse {
          Failure(new IOException(
            "Invalid version found. Expected one of %s, found %d." format (formats.keys mkString ",", version)))
        }
      }
    }

    def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment] = {
      readVersion(channel) flatMap { version =>
        formats get version map { format =>
          format.reader.readSegment(channel)
        } getOrElse {
          Failure(new IOException(
            "Invalid version found. Expected one of %s, found %d." format (formats.keys mkString ",", version)))
        }
      }
    }
  }
}
