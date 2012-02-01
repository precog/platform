package com.precog.yggdrasil

import com.precog.common._

import scalaz.effect._
import scala.collection.mutable

package object shard {
  
  type MetadataMap = mutable.Map[MetadataType, Metadata]
  type Checkpoints = mutable.Map[Int, Int]
  
  type MetadataIO = (ProjectionDescriptor, Seq[MetadataMap]) => IO[Unit]
  type CheckpointIO = Checkpoints => IO[Unit]

  type ProducerId = Int
  type SequenceId = Int

}
