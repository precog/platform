package com.precog
package daze

import yggdrasil._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext
import java.io.File

import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import IterateeT._

trait LevelDBMemoizationConfig {
  def memoizationBufferSize: Int
  def memoizationWorkDir: File
}

trait LevelDBMemoizationComponent extends YggConfigComponent with MemoizationComponent { component => 
  type YggConfig <: LevelDBMemoizationConfig

  sealed trait MemoContext extends MemoizationContext { 
    @volatile private var cache = Map.empty[Int, (Option[ProjectionDescriptor], Either[Vector[SEvent], File])]

    def expire(memoId: Int) = IO {
      component.synchronized { cache -= memoId }
    }

    def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext): Either[MemoizationContext.Memoizer[X], DatasetEnum[X, SEvent, IO]] = component.synchronized {
      cache.get(memoId) match {
        case Some((descriptor, Left(vector))) => 
          Right(DatasetEnum(Future(EnumeratorP.enumPStream[X, Vector[SEvent], IO](Stream(vector))), descriptor))
          
        case Some((descriptor, Right(file))) => 
          sys.error("disk-based memoization not yet supported") 

        case None => Left(
          new MemoizationContext.Memoizer[X] {
            def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = (iter: IterateeT[X, Vector[SEvent], F, A]) => {
              import MO._
              (iter zip (fold[X, Vector[SEvent], F, Vector[SEvent]](Vector.empty[SEvent]) { (v, ev) => v ++ ev })) map {
                case (result, vector) => 
                  component.synchronized { if (!cache.isDefinedAt(memoId)) {
                    cache += (memoId -> (d, Left(vector))) 
                  }}
                  result
              }
            }
          }
        )
      }
    }

    def purge = IO {}
  }

  def withMemoizationContext[A](f: MemoContext => A) = f(new MemoContext { })
}




// vim: set ts=4 sw=4 et:
