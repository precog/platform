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

