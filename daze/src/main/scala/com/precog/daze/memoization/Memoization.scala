package com.precog
package daze
package memoization

import yggdrasil._
import yggdrasil.serialization._

import akka.dispatch.Future
import akka.dispatch.ExecutionContext

import java.io._
import scalaz._
import scalaz.effect._
import scalaz.iteratee._
import IterateeT._


trait MemoizationEnvironment {
  type Dataset[E]
  type MemoContext <: MemoizationContext[Dataset]

  def withMemoizationContext[A](f: MemoContext => A): A
}

trait MemoCache {
  def expire(memoId: MemoId): Unit
  def purge: Unit
}

object MemoCache {
  object Noop extends MemoCache {
    def expire(memoId: Int) = ()
    def purge = ()
  }
}

trait MemoizationContext[Dataset[_]] {
  def cache: MemoCache
  def memoizing[A](memoId: Int)(implicit serialization: IncrementalSerialization[(Identities, A)]): Either[Dataset[A] => Future[Dataset[A]], Future[Dataset[A]]] 
  //def memosort[A](memoId: Int)(implicit serialization: SortSerialization[A], buffering: Buffering[A]): Either[Sortable[A] => Future[Sortable[A]], Future[Sortable[A]]]
}

/*
trait IterateeMemoizationContext extends MemoizationContext {
  trait Memoizer[X, A] {
    def apply[F[_], A](iter: IterateeT[X, A, F, A])(implicit MO: F |>=| IO): IterateeT[X, A, F, A]
  }

  def memoizing[X, A](memoId: Int)(implicit fs: IterateeFileSerialization[A], asyncContext: ExecutionContext): Either[Memoizer[X, A], EnumeratorP[X, A, IO]]
}

object IterateeMemoizationContext {
  trait Noop extends IterateeMemoizationContext {
    def memoizing[X, A](memoId: Int)(implicit fs: IterateeFileSerialization[A], asyncContext: ExecutionContext): Either[Memoizer[X, A], EnumeratorP[X, A, IO]] = Left(
      new Memoizer[X, A] {
        def apply[F[_], A](iter: IterateeT[X, A, F, A])(implicit MO: F |>=| IO) = iter
      }
    )
  }

  object Noop extends Noop {
    val cache = MemoCache.Noop
  }
}

trait BufferingContext {
  def cache: MemoCache

  def buffering[X, A, F[_]](memoId: Int)(implicit fs: IterateeFileSerialization[A], MO: F |>=| IO): IterateeT[X, A, F, EnumeratorP[X, A, IO]]
}

object BufferingContext {
  trait Memory extends BufferingContext {
    def bufferSize: Int
    def buffering[X, A, F[_]](memoId: Int)(implicit fs: IterateeFileSerialization[A], MO: F |>=| IO): IterateeT[X, A, F, EnumeratorP[X, A, IO]] = {
      import MO._
      import scalaz.std.list._
      take[X, A, F, List](bufferSize).map(l => EnumeratorP.enumPStream[X, A, IO](l.toStream))
    }
  }

  def memory(size: Int) = new Memory {
    val cache = MemoCache.Noop
    val bufferSize = size
  }
}

*/
// vim: set ts=4 sw=4 et:
