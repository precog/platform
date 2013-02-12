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
case class VersionedSegmentFormat(formats: Map[Int, SegmentFormat]) extends SegmentFormat {
  private val (version, format) = formats.maxBy(_._1)

  object writer extends SegmentWriter {
    def writeSegment(channel: WritableByteChannel, segment: Segment) = {
      def writeVersion(): Validation[IOException, PrecogUnit] = {
        val buffer = ByteBuffer.allocate(4)
        buffer.putInt(version)
        buffer.flip()

        try {
          while (buffer.remaining() > 0) {
            channel.write(buffer)
          }
          Success(PrecogUnit)
        } catch { case ioe: IOException =>
          Failure(ioe)
        }
      }

      for {
        _ <- writeVersion()
        _ <- format.writer.writeSegment(channel, segment)
      } yield PrecogUnit
    }
  }

  object reader extends SegmentReader {
    def readSegment(channel: ReadableByteChannel): Validation[IOException, Segment] = {
      def readVersion(): Validation[IOException, Int] = {
        val buffer = ByteBuffer.allocate(4)
        try {
          while (buffer.remaining() > 0) {
            channel.read(buffer)
          }
          Success(buffer.getInt())
        } catch { case ioe: IOException =>
          Failure(ioe)
        }
      }

      readVersion() flatMap { version =>
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
