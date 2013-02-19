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
  def structure: M[Set[ColumnRef]]
  def length: Long

  /** 
   * Get a block of data beginning with the first record with a key greater than
   * the specified key. If id.isEmpty, return a block starting with the minimum
   * key. Each resulting block should contain only the columns specified in the 
   * column set; if the set of columns is empty, return all columns.
   */
  def getBlockAfter(id: Option[Key], columns: Option[Set[ColumnRef]] = None)(implicit M: Monad[M]): M[Option[BlockProjectionData[Key, Block]]]
}

//trait RawProjectionModule[M[+_], Key, Block] extends ProjectionModule[M, Key, Block] { 
//  type Projection <: RawProjectionLike[M, Key, Block]
//  type ProjectionCompanion <: RawProjectionCompanionLike[M]
//
//  trait RawProjectionCompanionLike[M0[+_]] extends ProjectionCompanionLike[M0] { self =>
//    def close(p: Projection): M0[PrecogUnit]
//    def archive(d: ProjectionDescriptor): M0[Boolean]
//
//    override def liftM[T[_[+_], +_]](implicit T: Hoist[T], M0: Monad[M0]) = new RawProjectionCompanionLike[({ type λ[+α] = T[M0, α] })#λ] {
//      def apply(descriptor: ProjectionDescriptor) = self(descriptor).liftM[T]
//      def apply(path: Path) = self.apply(path).liftM[T]
//      def close(p: Projection) = self.close(p).liftM[T]
//      def archive(d: ProjectionDescriptor) = self.archive(d).liftM[T]
//    }
//  }
//}

trait RawProjectionLike[M[+_], Key, Block] extends ProjectionLike[M, Key, Block] {
  // def insert(id : Identities, v : Seq[CValue], shouldSync: Boolean = false): M[PrecogUnit]
  def insert(id : Identities, v : Seq[JValue]): M[PrecogUnit]
  def commit: M[PrecogUnit]
}

trait SortProjectionLike[M[+_], Key, Block] extends RawProjectionLike[M, Key, Block] {
  def descriptor = sys.error("Sort projections do not have full ProjectionDescriptors")
  def insert(id : Identities, v : Seq[JValue]) = sys.error("Insertion on sort projections is unsupported")
  def commit = sys.error("Commit on sort projections is unsupported")
}
