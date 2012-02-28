package com.precog.yggdrasil

import com.precog.common._

import scalaz.effect._
import scala.collection.mutable

package object shard {
  
  type MetadataMap = Map[MetadataType, Metadata]
  
  type MetadataIO = (ProjectionDescriptor, ColumnMetadata) => IO[Unit]

  type ColumnMetadata = Map[ColumnDescriptor, MetadataMap]

  object ColumnMetadata {
    val Empty = Map.empty[ColumnDescriptor, MetadataMap]
  }

}
