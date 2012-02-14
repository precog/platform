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

  private var cache = Map.empty[Int, (Option[ProjectionDescriptor], Either[Vector[SEvent], File])]

  class MemoContext extends MemoizationContext { 
    def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext): Either[MemoizationContext.Memoizer[X], DatasetEnum[X, SEvent, IO]] = component.synchronized {
      cache.get(memoId) match {
        case Some((descriptor, Left(vector))) => 
          Right(DatasetEnum(Future(EnumeratorP.enumPStream[X, SEvent, IO](vector.toStream)), descriptor))
          
        case Some((descriptor, Right(file))) => 
          sys.error("disk-based memoization not yet supported") 

        case None => Left(
          new MemoizationContext.Memoizer[X] {
            def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = (iter: IterateeT[X, SEvent, F, A]) => {
              import MO._
              (iter zip (fold[X, SEvent, F, Vector[SEvent]](Vector.empty[SEvent]) { (v, ev) => v :+ ev })) map {
                case (result, vector) => 
                  component.synchronized { if (!cache.isDefinedAt(memoId)) {
                    println("Caching result for id " + memoId + ": " + vector)
                    cache += (memoId -> (d, Left(vector))) 
                  }}
                  result
              }
            }
          }
        )
      }
    }
  }
}




// vim: set ts=4 sw=4 et:
