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

trait MemoizationContext {
  trait Memoizer[X, E] {
    def memoizing[F[_], A](iter: IterateeT[X, E, F, A])(implicit MO: F |>=| IO): IterateeT[X, E, F, A]
  }

  def apply[X, E](memoId: Int)(implicit fs: FileSerialization[E], asyncContext: ExecutionContext): Either[Memoizer[X, E], EnumeratorP[X, E, IO]]
  def expire(memoId: Int): IO[Unit]
  def purge: IO[Unit]
}

trait Buffering[E] {
  def apply[X, F[_]](implicit MO: F |>=| IO): IterateeT[X, E, F, EnumeratorP[X, E, IO]]
}

object MemoizationContext {
  object Noop extends MemoizationContext {
    def apply[X, E](memoId: Int)(implicit fs: FileSerialization[E], asyncContext: ExecutionContext): Either[Memoizer[X, E], EnumeratorP[X, E, IO]] = Left(
      new Memoizer[X, E] {
        def memoizing[F[_], A](iter: IterateeT[X, E, F, A])(implicit MO: F |>=| IO) = iter
      }
    )

    def expire(memoId: Int) = IO(())
    def purge = IO(())
  }
}

trait MemoizationComponent {
  type MemoContext <: MemoizationContext

  def withMemoizationContext[A](f: MemoContext => A): A
}

// vim: set ts=4 sw=4 et:
