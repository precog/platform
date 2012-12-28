package com.precog.ingest
package service

import com.precog.common.ingest._

import blueeyes.bkka._
import akka.actor.ActorSystem
import akka.dispatch.{ Future, ExecutionContext, Await }
import akka.util.Duration

import blueeyes.core.data._

import org.specs2._
import org.specs2.mutable.Specification
import org.scalacheck.{ Shrink, Arbitrary, Gen }

import scalaz._

import java.io._
import java.nio.ByteBuffer
import java.util.zip._


class DecompressByteChunkSpec extends Specification with ScalaCheck {
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

  def xzip(data: String): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val out = new ZipOutputStream(baos)
    out.putNextEntry(new ZipEntry("foo"))
    out.write(data.getBytes("UTF-8"))
    out.closeEntry()
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

  
  protected implicit val M = new FutureMonad(ExecutionContext.defaultExecutionContext)

  private def mash(prefix: Array[Byte], stream: StreamT[Future, ByteBuffer]):
      Future[Array[Byte]] = {

    def concat(store: Array[Byte], bb: ByteBuffer): Array[Byte] = {
      val arr = new Array[Byte](store.length + bb.remaining)
      System.arraycopy(store, 0, arr, 0, store.length)
      bb.get(arr, store.length, bb.remaining)
      arr
    }

    stream.uncons.flatMap {
      case Some((buf, tail)) => mash(concat(prefix, buf), tail)
      case None => Future(prefix)
    }
  }

  def bbToArray(bb: ByteBuffer): Array[Byte] = {
    val arr = new Array[Byte](bb.remaining)
    bb.get(arr)
    arr
  }

  def reassemble(chunk: ByteChunk): Array[Byte] = {
    def assemble(chunk: ByteChunk): Future[Array[Byte]] = chunk match {
      case Left(bb) => Future(bbToArray(bb))
      case Right(stream) => mash(new Array[Byte](0), stream)
    }
    val fba = assemble(chunk)
    Await.result(fba, Duration(1, "seconds"))
  }

  def emptybb = ByteBuffer.wrap(new Array[Byte](0))

  val EmptyByteChunk: ByteChunk = Left(emptybb)

  import scalaz.StreamT
  def split(bytes: Array[Byte], n: Int = 1): ByteChunk = {
    val stream = bytes.grouped(n).map(ByteBuffer.wrap).
      foldRight(StreamT(Future(StreamT.Done)):StreamT[Future, ByteBuffer]) {
        (bytes, chunk) => StreamT(Future(StreamT.Yield(bytes, chunk)))
      }
    Right(stream)
  }

  def emptystream = StreamT.empty[Future, ByteBuffer]

  def pad(bb: ByteBuffer) = bb :: emptybb :: emptystream

  def spaceout(bc: ByteChunk): ByteChunk = Right(bc.fold(pad, _.flatMap(pad)))

  "InflateByteChunk" should {
    "reinflate trivial ByteChunk" in {
      val chunk: ByteChunk = Left(ByteBuffer.wrap(deflate("Hello, world!")))
      val inflated = reassemble((new InflateByteChunk).apply(chunk))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes" in {
      val chunk: ByteChunk = split(deflate("Hello, world!"))
      val inflated = reassemble((new InflateByteChunk).apply(chunk))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes with empty parts" in {
      val chunk: ByteChunk = spaceout(split(deflate("Hello, world!")))
      val inflated = reassemble((new InflateByteChunk).apply(chunk))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate arbitrary strings" in {
      check { (s: String) =>
        val inflated = reassemble((new InflateByteChunk).apply(split(deflate(s))))
        new String(inflated, "UTF-8") must_== s
      }
    }
  }

  "GunzipByteChunk" should {
    "unzip trivial ByteChunk" in {
      val chunk: ByteChunk = Left(ByteBuffer.wrap(gzip("Hello, world!")))
      val fbc = (new GunzipByteChunk).apply(chunk)
      val inflated = reassemble(fbc)
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes" in {
      val inflated = reassemble((new GunzipByteChunk).apply(split(gzip("Hello, world!"))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes with empty parts" in {
      val inflated = reassemble((new GunzipByteChunk).apply(spaceout(split(gzip("Hello, world!")))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate arbitrary strings" in { check { (s: String) =>
      val inflated = reassemble((new GunzipByteChunk).apply(split(gzip(s))))
      new String(inflated, "UTF-8") must_== s
    } }
  }

  "UnzipByteChunk" should {
    "unzip trivial ByteChunk" in {
      val chunk: ByteChunk = Left(ByteBuffer.wrap(xzip("Hello, world!")))
      val fbc = (new UnzipByteChunk).apply(chunk)
      val inflated = reassemble(fbc)
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes" in {
      val inflated = reassemble((new UnzipByteChunk).apply(split(xzip("Hello, world!"))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate chunked bytes with empty parts" in {
      val inflated = reassemble((new UnzipByteChunk).apply(spaceout(split(xzip("Hello, world!")))))
      new String(inflated, "UTF-8") must_== "Hello, world!"
    }

    "reinflate arbitrary strings" in { check { (s: String) =>
      val inflated = reassemble((new UnzipByteChunk).apply(split(xzip(s))))
      new String(inflated, "UTF-8") must_== s
    } }
  }

  step { actorSystem.shutdown() }
}
