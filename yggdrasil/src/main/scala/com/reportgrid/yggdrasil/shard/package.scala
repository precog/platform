package com.reportgrid.yggdrasil

import com.reportgrid.common._

import scalaz.effect._
import scala.collection.mutable

package object shard {
  
  type MetadataMap = mutable.Map[MetadataType, Metadata]
  type Checkpoints = mutable.Map[Int, Int]
  
  type MetadataIO = (ProjectionDescriptor, Seq[MetadataMap]) => IO[Unit]
  type CheckpointIO = Checkpoints => IO[Unit]

  val echoMetadataIO = (descriptor: ProjectionDescriptor, metadata: Seq[MetadataMap]) => IO { 
    println("Saving metadata entry")
    println(descriptor)
    println(metadata)
  }

  val echoCheckpointIO = (checkpoints: Checkpoints) => IO {
    println("Saving checkpoints")
    println(checkpoints) 
  }
}
