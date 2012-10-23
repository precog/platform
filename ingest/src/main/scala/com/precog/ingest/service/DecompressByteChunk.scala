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
package com.precog.ingest.service

import akka.dispatch.{ Future, ExecutionContext }

import blueeyes.core.data.{ Chunk, ByteChunk }

import java.io.{ InputStream, ByteArrayOutputStream, IOException, EOFException }
import java.util.zip.Inflater

import com.weiglewilczek.slf4s.Logging

trait DecompressByteChunk extends Logging {

  def nowrap: Boolean

  def apply(byteStream: ByteChunk)(implicit ctx: ExecutionContext): ByteChunk

  sealed trait InflateResult
  case class More(bytes: Array[Byte], next: Option[Future[ByteChunk]]) extends InflateResult
  case class Done(bytes: Array[Byte], remaining: ByteChunk) extends InflateResult

  def createInflater(): ByteChunk => InflateResult = {
    val inflater: Inflater = new Inflater(nowrap)
    val bytes = new Array[Byte](1024)
    val out = new ByteArrayOutputStream()

    { (chunk: ByteChunk) => 

      val data = if (chunk.data.length > 0) {

        logger.debug("Proposing %d bytes for inflation." format chunk.data.length)

        inflater.setInput(chunk.data)

        out.reset()
        var len = inflater.inflate(bytes)
        while (len > 0) {
          out.write(bytes, 0, len)
          len = inflater.inflate(bytes)
        }
        out.toByteArray()

      } else {
        new Array[Byte](0)
      }

      logger.debug("Inflated bytes" format data.length)

      if (inflater.needsInput()) {
        More(data, chunk.next)

      } else {
        val remaining = inflater.getRemaining()
        val leftover = new Array[Byte](remaining)
        System.arraycopy(chunk.data, chunk.data.length - remaining, leftover, 0, remaining)
        Done(data, Chunk(leftover, chunk.next))
      }
    }
  }
}

case object InflateByteChunk extends DecompressByteChunk {
  val nowrap = false

  def apply(byteStream: ByteChunk)(implicit ctx: ExecutionContext): ByteChunk = {
    val inflateStream = createInflater()

    def inflate(chunk: ByteChunk): ByteChunk = {
      inflateStream(chunk) match {
        case More(bytes, next) =>
          Chunk(bytes, next map (_ map (inflate(_))))
        case Done(bytes, remaining) =>
          Chunk(bytes, Some(Future(apply(remaining))))
      }
    }

    inflate(byteStream)
  }
}

case object GunzipByteChunk extends DecompressByteChunk {
  val nowrap = true

  private val TextFlag = 0x01
  private val CrcFlag = 0x02
  private val ExtraFlag = 0x04
  private val NameFlag = 0x08
  private val CommentFlag = 0x10

  def apply(byteStream: ByteChunk)(implicit ctx: ExecutionContext): ByteChunk = {
    val inflateStream = createInflater()

    def inflate(chunk: ByteChunk): ByteChunk = {
      inflateStream(chunk) match {
        case More(bytes, next) =>
          Chunk(bytes, next map (_ map (inflate(_))))
        case Done(bytes, remaining) =>
          Chunk(bytes, Some(Future(finish(remaining))))
      }
    }

    inflate(init(byteStream))
  }

  /** Reads a fixed amount of data from a `ByteChunk`. */
  private def read(len: Int, chunk: ByteChunk)(implicit
      ctx: ExecutionContext): Future[(Array[Byte], ByteChunk)] = {

    def rec(out: ByteArrayOutputStream, len: Int, chunk: ByteChunk): Future[(Array[Byte], ByteChunk)] = {
      if (chunk.data.length >= len) {
        out.write(chunk.data, 0, len)
        val bytes = out.toByteArray()
        val leftover = new Array[Byte](chunk.data.length - len)
        System.arraycopy(chunk.data, len, leftover, 0, chunk.data.length - len)
        Future((bytes, Chunk(leftover, chunk.next)))

      } else {
        out.write(chunk.data)

        chunk.next map {
          _ flatMap (rec(out, len - chunk.data.length, _))
        } getOrElse {
          throw new EOFException("Unexpected end of file while reading GZIP header.")
        }
      }
    }

    val out = new ByteArrayOutputStream()
    rec(out, len, chunk)
  }

  /** Returns a `ByteChunk` with `len` bytes removed. */
  private def skip(len: Int, chunk: ByteChunk): ByteChunk = {
    if (chunk.data.length < len) {
      Chunk(new Array[Byte](0), chunk.next map (_ map (skip(len - chunk.data.length, _))))
    } else {
      val leftover = new Array[Byte](chunk.data.length - len)
      System.arraycopy(chunk.data, len, leftover, 0, leftover.length)
      Chunk(leftover, chunk.next)
    }
  }

  /** Returns a `ByteChunk` with all bytes up and including the frist 0 byte removed. */
  private def skipString(chunk: ByteChunk): ByteChunk = {
    var i = 0
    while (i < chunk.data.length && chunk.data(i) != 0) {
      i += 1
    }

    if (i == chunk.data.length) {
      Chunk(new Array[Byte](0), chunk.next map (_ map (skipString(_))))
    } else {
      val leftover = new Array[Byte](chunk.data.length - i - 1)
      System.arraycopy(chunk.data, i + 1, leftover, 0, leftover.length)
      Chunk(leftover, chunk.next)
    }
  }

  def skipExtra(chunk: ByteChunk)(implicit ctx: ExecutionContext): ByteChunk = {
    Chunk(new Array[Byte](0), Some(read(2, chunk) map {
      case (bytes, chunk) =>
        val len = ushort(bytes, 0)
        skip(len, chunk)
    }))
  }

  private def ushort(bytes: Array[Byte], offset: Int): Int =
    ((bytes(offset + 1) & 255) << 8) | (bytes(offset) & 255)

  def finish(chunk: ByteChunk)(implicit ctx: ExecutionContext): ByteChunk = {
    def restartOrFinish(chunk: ByteChunk): ByteChunk = if (chunk.data.length > 0) {
      apply(chunk)
    } else {
      Chunk(chunk.data, chunk.next map (_ map (restartOrFinish(_))))
    }

    restartOrFinish(skip(8, chunk))
  }

  def init(chunk: ByteChunk)(implicit ctx: ExecutionContext): ByteChunk = {

    def maybeSkipExtra(flags: Int): ByteChunk => ByteChunk =
      if ((flags & ExtraFlag) != 0) skipExtra(_) else identity

    def maybeSkipName(flags: Int): ByteChunk => ByteChunk =
      if ((flags & NameFlag) != 0) skipString(_) else identity

    def maybeSkipComment(flags: Int): ByteChunk => ByteChunk =
      if ((flags & CommentFlag) != 0) skipString(_) else identity

    def maybeSkipCrc(flags: Int): ByteChunk => Future[ByteChunk] = {
      chunk => if ((flags & CrcFlag) != 0) {
        read(2, chunk) map (_._2)
      } else Future(chunk)
    }

    Chunk(new Array[Byte](0), Some(read(10, chunk) flatMap {
      case (h0, chunk) =>
        if (h0(0) != 31 || (h0(1) & 255) != 139) {
          throw new IOException("Expected a GZIP file, but magic # is wrong.")
        } else if (h0(2) != 8) {
          throw new IOException("Unsupported GZIP compression format used.")
        }
        val flags = h0(3) & 0xFF

        (maybeSkipExtra(flags) andThen
          maybeSkipName(flags) andThen
          maybeSkipComment(flags) andThen
          maybeSkipCrc(flags))(chunk)
    }))
  }
}

