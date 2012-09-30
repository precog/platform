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

import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import akka.util.Duration

import blueeyes.core.data._

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck.{ Shrink, Arbitrary, Gen }

import java.io._
import java.util.zip._


class DecompressByteChunkSpec extends Specification with ScalaCheck {
  // implicit def ctx = ExecuteContext(actorSystem)
  implicit val actorSystem = ActorSystem("testDecompressByteChunk")

  override def is = args(sequential = true) ^ super.is

  def deflate(data: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new DeflaterOutputStream(baos)
    out.write(data.getBytes("UTF-8"))
    out.finish()
    baos.toByteArray()
  }

  def inflate(data: Array[Byte]): String = {
    val bais = new ByteArrayInputStream(data)
    val in = new InflaterInputStream(bais)
    val buf = new Array[Byte](1024)
    val out = new ByteArrayOutputStream()

    var len = in.read(buf)
    while (len >= 0) {
      out.write(buf, 0, len)
      len = in.read(buf)
    }

    new String(out.toByteArray(), "UTF-8")
  }

  def gzip(data: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new GZIPOutputStream(baos)
    out.write(data.getBytes("UTF-8"))
    out.finish()
    baos.toByteArray()
  }

  def gunzip(data: Array[Byte]): String = {
    val bais = new ByteArrayInputStream(data)
    val in = new GZIPInputStream(bais)
    val buf = new Array[Byte](1024)
    val out = new ByteArrayOutputStream()

    var len = in.read(buf)
    while (len >= 0) {
      out.write(buf, 0, len)
      len = in.read(buf)
    }

    new String(out.toByteArray(), "UTF-8")
  }

  def reassemble(chunk: ByteChunk): Array[Byte] = {
    val out = new ByteArrayOutputStream()

    def assemble(chunk: ByteChunk): Future[Array[Byte]] = {
      out.write(chunk.data)
      chunk.next map { _ flatMap (assemble(_)) } getOrElse {
        Future(out.toByteArray())
      }
    }

    Await.result(assemble(chunk), Duration(1, "seconds"))
  }

  val EmptyByteChunk = Chunk(new Array[Byte](0), None)

  def split(bytes: Array[Byte], n: Int = 1): ByteChunk = {
    (bytes grouped n).foldRight(EmptyByteChunk) {
      case (bytes, chunk) => Chunk(bytes, Some(Future(chunk)))
    }
  }

  def spaceout(chunk: ByteChunk): ByteChunk = {
    val nextChunk = Chunk(chunk.data, chunk.next map (_ map (spaceout(_))))
    Chunk(new Array[Byte](0), Some(Future(nextChunk)))
  }

  "InflateByteChunk" should {
    "reinflate trivial ByteChunk" in {
      val inflated = reassemble(InflateByteChunk(Chunk(deflate("Hello, world!"), None)))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes" in {
      val inflated = reassemble(InflateByteChunk(split(deflate("Hello, world!"))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes with empty parts" in {
      val inflated = reassemble(InflateByteChunk(spaceout(split(deflate("Hello, world!")))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate concatenated bytes produces concatenated string" in {
      val bytes = deflate("Hello, ") ++ deflate("world!")
      val inflated = reassemble(InflateByteChunk(split(bytes, 2)))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate arbitrary strings" in { check { (s: String) =>
      val inflated = reassemble(InflateByteChunk(split(deflate(s))))
      new String(inflated, "UTF-8") must_== s
    } }
  }

  "GunzipByteChunk" should {
    "unzip trivial ByteChunk" in {
      val inflated = reassemble(GunzipByteChunk(Chunk(gzip("Hello, world!"), None)))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes" in {
      val inflated = reassemble(GunzipByteChunk(split(gzip("Hello, world!"))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes with empty parts" in {
      val inflated = reassemble(GunzipByteChunk(spaceout(split(gzip("Hello, world!")))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate concatenated bytes produces concatenated string" in {
      val bytes = gzip("Hello, ") ++ gzip("world!")
      val inflated = reassemble(GunzipByteChunk(split(bytes, 2)))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate arbitrary strings" in { check { (s: String) =>
      val inflated = reassemble(GunzipByteChunk(split(gzip(s))))
      new String(inflated, "UTF-8") must_== s
    } }
  }

  step { actorSystem.shutdown() }
}

