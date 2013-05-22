package com.precog.yggdrasil

import com.precog.common._
import com.precog.util.PrecogUnit

import blueeyes.json.JValue

import scalaz._
import scalaz.syntax.monad._

trait ProjectionModule[M[+_], Key, Block] {
  type Projection <: ProjectionLike[M, Key, Block]
  type ProjectionCompanion <: ProjectionCompanionLike[M]

  def Projection: ProjectionCompanion

  trait ProjectionCompanionLike[M0[+_]] { self =>
    def apply(path: Path): M0[Option[Projection]]

    def liftM[T[_[+_], +_]](implicit T: Hoist[T], M0: Monad[M0]) = new ProjectionCompanionLike[({ type λ[+α] = T[M0, α] })#λ] {
      def apply(path: Path) = self.apply(path).liftM[T]
    }
  }
}


case class BlockProjectionData[Key, Block](minKey: Key, maxKey: Key, data: Block)

trait ProjectionLike[M[+_], Key, Block] {
  def structure(implicit M: Monad[M]): M[Set[ColumnRef]]
  def length: Long

  /**
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isEmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]] = None)(implicit M: Monad[M]): M[Option[BlockProjectionData[Key, Block]]]

  def getBlockStream(columns: Option[Set[ColumnRef]])(implicit M: Monad[M]): StreamT[M, Block] = {
    StreamT.unfoldM[M, Block, Option[Key]](None) { key =>
      getBlockAfter(key, columns) map {
        _ map { case BlockProjectionData(_, maxKey, block) => (block, Some(maxKey)) }
      }
    }
  }
}

