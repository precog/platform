package com.precog.niflheim

import com.precog.common._
import com.precog.common.json._

import java.lang.ref.SoftReference

import java.io.{ File, IOException }
import java.io.FileInputStream
import java.nio.channels._

import scalaz._
import scalaz.syntax.traverse._
import scalaz.std.list._

final class CookedReader(metadata: File, blockFormat: CookedBlockFormat, segmentFormat: SegmentFormat) {

  private val lock = new AnyRef { }

  @volatile
  private var block: SoftReference[List[(SegmentId, File)]] = null

  private def maybeBlock = if (block != null) block.get() else null

  private def read[A](file: File)(f: ReadableByteChannel => A): A = {
    val channel = new FileInputStream(file).getChannel()
    try {
      f(channel)
    } finally {
      channel.close()
    }
  }

  private def loadFromDisk(): Validation[IOException, List[(SegmentId, File)]] = {
    read(metadata) { channel =>
      val segsV = blockFormat.readCookedBlock(channel) map (_.toList)
      segsV foreach { segs0 =>
        block = new SoftReference(segs0)
      }
      segsV
    }
  }

  def segments: Validation[IOException, List[(SegmentId, File)]] = {
    val segs = maybeBlock
    if (segs != null) {
      Success(segs)
    } else {
      lock.synchronized {
        val block = maybeBlock
        if (block == null) {
          loadFromDisk()
        } else {
          Success(block)
        }
      }
    }
  }

  private def segmentsByCPath: Validation[IOException, Map[CPath, List[File]]] = segments map { segs0 =>
    segs0.groupBy(_._1.cpath).map { case (cpath, segs1) =>
      (cpath, segs1 map (_._2))
    }.toMap
  }

  def blockids: Validation[IOException, Set[Long]] = segments map { segs =>
    segs.map(_._1.blockid).toSet
  }

  def load(paths: List[CPath]): ValidationNEL[IOException, List[(CPath, List[Segment])]] = {
    segmentsByCPath.toValidationNEL flatMap { (segsByPath: Map[CPath, List[File]]) =>
      paths.map { path =>
        val v: ValidationNEL[IOException, List[Segment]] = segsByPath.getOrElse(path, Nil).map { file =>
          read(file) { channel =>
            segmentFormat.reader.readSegment(channel).toValidationNEL
          }
        }.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, Segment]
        v map (path -> _)
      }.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, (CPath, List[Segment])]
    }
  }
}
