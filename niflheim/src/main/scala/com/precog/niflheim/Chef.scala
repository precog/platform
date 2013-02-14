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
case class Cooked(blockid: Long, seqId: Long, root: File, files: Seq[(SegmentId, File)])

final case  class Chef(format: SegmentFormat) extends Actor {
  private def typeCode(ctype: CType): String = CType.nameOf(ctype)

  def prefix(id: Segment): String = {
    val pathHash = id.cpath.hashCode.toString
    id.blockid + "-" + pathHash + "-" + typeCode(id.ctype)
  }

  def cook(root: File, segments: List[Segment]): ValidationNEL[IOException, List[(SegmentId, File)]] = {
    val files = segments map { seg =>
      val file = File.createTempFile(prefix(seg), ".cooked", root)
      val channel: WritableByteChannel = new FileOutputStream(file).getChannel()
      val result = format.writer.writeSegment(channel, seg) map { _ => (seg.id, file) }
      channel.close()
      result.toValidationNEL
    }

    files.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, (SegmentId, File)]
  }

  def receive = {
    case Prepare(blockid, seqId, root, source) =>
      cook(root, source.snapshot.segments) match {
        case Success(files) =>
          sender ! Cooked(blockid, seqId, root, files)
        case Failure(_) =>
          sender ! Spoilt(blockid, seqId)
      }
  }
}

