package com.precog.yggdrasil

import com.precog.common._

import scalaz.Monoid
import scala.collection.mutable

package object metadata {
  type MetadataMap = Map[MetadataType, Metadata]
  
  type ColumnMetadata = Map[ColumnDescriptor, MetadataMap]

  object ColumnMetadata {
    val Empty = Map.empty[ColumnDescriptor, MetadataMap]

    implicit object monoid extends Monoid[ColumnMetadata] {
      val zero = Empty

      def append(m1: ColumnMetadata, m2: => ColumnMetadata): ColumnMetadata = {
        m1.foldLeft(m2) {
          case (acc, (descriptor, mmap)) =>
            val currentMmap: MetadataMap = acc.getOrElse(descriptor, Map.empty[MetadataType, Metadata])
            val newMmap: MetadataMap = mmap.foldLeft(currentMmap) {
              case (macc, (mtype, m)) => 
                macc + (mtype -> macc.get(mtype).flatMap(_.merge(m)).getOrElse(m))
            }

            acc + (descriptor -> newMmap)
        }
      }
    }
  }
}
