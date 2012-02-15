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
  def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext): Either[MemoizationContext.Memoizer[X], DatasetEnum[X, SEvent, IO]]
  def expire(memoId: Int): IO[Unit]
}

object MemoizationContext {
  trait Memoizer[X] {
    def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO): IterateeT[X, SEvent, F, A] => IterateeT[X, SEvent, F, A]
  }

  object Memoizer {
    def noop[X]: Memoizer[X] = new Memoizer[X] {
      def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = iter => iter
    }
  }

  trait Noop extends MemoizationContext {
    def apply[X](memoId: Int)(implicit asyncContext: ExecutionContext) = Left(Memoizer.noop[X])
    def expire(memoId: Int) = IO(())
  }

  object Noop extends Noop
}

trait MemoizationComponent {
  type MemoContext <: MemoizationContext

  def memoizationContext: MemoContext
}

// vim: set ts=4 sw=4 et:
