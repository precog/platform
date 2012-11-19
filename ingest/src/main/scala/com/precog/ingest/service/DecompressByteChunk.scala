package com.precog.ingest.service

import blueeyes.bkka._
import akka.dispatch.{ExecutionContext, Future, Promise}

import blueeyes.core.data.{ Chunk, ByteChunk }

import java.io._
import java.nio._
import java.nio.channels._
import java.util.zip._

import com.weiglewilczek.slf4s.Logging

import java.nio.ByteBuffer
import scalaz._

abstract class DecompressByteChunk(implicit executor: ExecutionContext) extends Logging {
  protected implicit val M = new FutureMonad(executor)

  protected def inChannel(in: InputStream): ReadableByteChannel

  def decompress(dataChunk: ByteChunk, chunkSize: Int = 8192): ByteChunk = {
    val stream: StreamT[Future, ByteBuffer] = dataChunk match {
      case Left(bb) => StreamT(Future(StreamT.Yield(bb, StreamT(Future(StreamT.Done)))))
      case Right(stream) => stream
    }

    val inPipe = new PipedInputStream()
    val outStream = new BufferedOutputStream(new PipedOutputStream(inPipe))
    val inc = inChannel(new BufferedInputStream(inPipe))
    val fu = writeDecompressed(stream, outStream)

    val sfu = fu :: StreamT.empty[Future, Unit]

    val result: StreamT[Future, ByteBuffer] = sfu flatMap { unit =>
      StreamT.unfoldM[Future, ByteBuffer, Option[ReadableByteChannel]](Some(inc)) {
        case Some(in) => Future {
          val buffer = ByteBuffer.allocate(chunkSize)
          val read = in.read(buffer)
          buffer.flip()
          if (read == -1) {
            in.close()
            Some((buffer, None))
          } else {
            Some((buffer, Some(in)))
          }
        }
          
        case None => {
          Promise.successful(None)
        }
      }
    }

    Right(result)
  }

  protected def writeDecompressed(stream: StreamT[Future, ByteBuffer], out: OutputStream): Future[Unit] = {
    val c = Channels.newChannel(out)

    def writeChannel(stream: StreamT[Future, ByteBuffer]): Future[Unit] = {
      stream.uncons map {
        case Some((buffer, tail)) =>
          c.write(buffer)
          writeChannel(tail)
        case None =>
          Future(c.close())
      }
    }

    writeChannel(stream)
  }

  def apply(byteStream: ByteChunk): ByteChunk = decompress(byteStream)
}

case class InflateByteChunk(implicit ctx: ExecutionContext)
  extends DecompressByteChunk()(ctx) {
  protected def inChannel(in: InputStream): ReadableByteChannel =
    Channels.newChannel(new InflaterInputStream(in))
}

case class GunzipByteChunk(implicit ctx: ExecutionContext)
  extends DecompressByteChunk()(ctx) {
  protected def inChannel(in: InputStream): ReadableByteChannel = {
    // work-around because GZIPInputStream needs to read the header
    // during the constructor.
    val gzis = new InputStream {
      lazy val gz = new GZIPInputStream(in)
      def read() = gz.read()
      override def read(b: Array[Byte]) = gz.read(b)
      override def read(b: Array[Byte], off: Int, len: Int) = gz.read(b, off, len)
    }
    Channels.newChannel(gzis)
  }
}

case class UnzipByteChunk(implicit ctx: ExecutionContext)
  extends DecompressByteChunk()(ctx) {
  protected def inChannel(in: InputStream): ReadableByteChannel = {
    val zis = new InputStream {
      val z = new ZipInputStream(in)
      lazy val entry = z.getNextEntry()
      def read() = {
        if (entry == null) sys.error("no files found")
        z.read()
      }
      override def read(b: Array[Byte]) = {
        if (entry == null) sys.error("no files found")
        z.read(b)
      }
      override def read(b: Array[Byte], off: Int, len: Int) = {
        if (entry == null) sys.error("no files found")
        z.read(b, off, len)
      }
    }
    Channels.newChannel(zis)
  }
}
