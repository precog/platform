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

object CookedReader { 
  def load(metadataFile: File, blockFormat: CookedBlockFormat = VersionedCookedBlockFormat(Map(1 -> V1CookedBlockFormat)), segmentFormat: SegmentFormat = VersionedSegmentFormat(Map(1 -> V1SegmentFormat))): CookedReader = 
    new CookedReader(metadataFile, blockFormat, segmentFormat)
}

final class CookedReader(metadataFile: File, blockFormat: CookedBlockFormat, segmentFormat: SegmentFormat) extends StorageReader {

  private val lock = new AnyRef { }

  def isStable: Boolean = true

  @volatile
  private var block: SoftReference[CookedBlockMetadata] = null

  private def maybeBlock = if (block != null) block.get() else null

  private def read[A](file: File)(f: ReadableByteChannel => A): A = {
    val channel = new FileInputStream(file).getChannel()
    try {
      f(channel)
    } finally {
      channel.close()
    }
  }

  private def loadFromDisk(): Validation[IOException, CookedBlockMetadata] = {
    read(metadataFile) { channel =>
      val segsV = blockFormat.readCookedBlock(channel)
      segsV foreach { segs0 =>
        block = new SoftReference(segs0)
      }
      segsV
    }
  }

  def id: Long = metadata.valueOr(throw _).blockid
  def length: Int = metadata.valueOr(throw _).length

  def snapshot(pathConstraint: Option[Set[ColumnRef]]): Block = {
    val segments: Seq[Segment] = pathConstraint map { refs =>
      load(refs.toList).map({ segs =>
        segs flatMap (_._2)
      }).valueOr { nel => throw nel.head }
    } getOrElse {
      metadata.valueOr(throw _).segments map { case (segId, file) =>
        read(file) { channel =>
          segmentFormat.reader.readSegment(channel)
        }.valueOr(throw _)
      }
    }

    Block(id, segments, isStable)
  }

  def structure: Iterable[ColumnRef] = metadata.valueOr(throw _).segments map {
    case (segId, _) => ColumnRef(segId.cpath, segId.ctype)
  }

  def metadata: Validation[IOException, CookedBlockMetadata] = {
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

  private def segmentsByCPath: Validation[IOException, Map[ColumnRef, List[File]]] = metadata map { md =>
    md.segments.groupBy( s => ColumnRef(s._1.cpath, s._1.ctype)).map { case (info, segs) =>
      (info, segs.map(_._2).toList)
    }.toMap
  }

  def load(refs: List[ColumnRef]): ValidationNEL[IOException, List[(ColumnRef, List[Segment])]] = {
    segmentsByCPath.toValidationNEL flatMap { (segsByPath: Map[ColumnRef, List[File]]) =>
      refs.map { case ref =>
        val v: ValidationNEL[IOException, List[Segment]] = segsByPath.getOrElse(ref, Nil).map { file =>
          read(file) { channel =>
            segmentFormat.reader.readSegment(channel).toValidationNEL
          }
        }.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, Segment]
        v map (ref -> _)
      }.sequence[({ type λ[α] = ValidationNEL[IOException, α] })#λ, (ColumnRef, List[Segment])]
    }
  }
}
