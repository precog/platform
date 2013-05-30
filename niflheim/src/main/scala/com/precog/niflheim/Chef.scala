package com.precog.niflheim

import com.precog.common._
import com.precog.util._

import java.io._
import java.nio.channels.WritableByteChannel

import akka.actor._

import scalaz._
import scalaz.syntax.traverse._
import scalaz.std.list._

case class Prepare(blockid: Long, seqId: Long, root: File, source: StorageReader)
case class Spoilt(blockid: Long, seqId: Long)
case class Cooked(blockid: Long, seqId: Long, root: File, metadata: File)

final case class Chef(blockFormat: CookedBlockFormat, format: SegmentFormat) extends Actor {
  private def typeCode(ctype: CType): String = CType.nameOf(ctype)

  def prefix(id: Segment): String = {
    val pathHash = id.cpath.hashCode.toString
    "segment-" + id.blockid + "-" + pathHash + "-" + typeCode(id.ctype)
  }

  def cook(root: File, reader: StorageReader): ValidationNel[IOException, File] = {
    assert(root.exists)
    assert(root.isDirectory)
    assert(root.canWrite)
    val files0 = reader.snapshot(None).segments map { seg =>
      val file = File.createTempFile(prefix(seg), ".cooked", root)
      val relativized = new File(file.getName)
      val channel: WritableByteChannel = new FileOutputStream(file).getChannel()
      val result = try {
        format.writer.writeSegment(channel, seg) map { _ => (seg.id, relativized) }
      } finally {
        channel.close()
      }
      result.toValidationNel
    }

    val files = files0.toList.sequence[({ type λ[α] = ValidationNel[IOException, α] })#λ, (SegmentId, File)]
    files flatMap { segs =>
      val metadata = CookedBlockMetadata(reader.id, reader.length, segs.toArray)
      val mdFile = File.createTempFile("block-%08x".format(reader.id), ".cookedmeta", root)
      val channel = new FileOutputStream(mdFile).getChannel()
      try {
        blockFormat.writeCookedBlock(channel, metadata).toValidationNel.map { _ : PrecogUnit =>
          new File(mdFile.getName)
        }
      } finally {
        channel.close()
      }
    }
  }

  def receive = {
    case Prepare(blockid, seqId, root, source) =>
      cook(root, source) match {
        case Success(file) =>
          sender ! Cooked(blockid, seqId, root, file)
        case Failure(_) =>
          sender ! Spoilt(blockid, seqId)
      }
  }
}

