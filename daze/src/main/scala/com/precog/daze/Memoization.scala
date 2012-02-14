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

trait MemoizationContext[X] {
  trait Memoizer[X] {
    def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO): IterateeT[X, SEvent, F, A] => IterateeT[X, SEvent, F, A]
  }

  def apply(memoId: Int)(implicit asyncContext: ExecutionContext): Either[Memoizer[X], DatasetEnum[X, SEvent, IO]]

  val noopMemoizer: Memoizer[X] = new Memoizer[X] {
    def apply[F[_], A](d: Option[ProjectionDescriptor])(implicit MO: F |>=| IO) = iter => iter
  }
}

object MemoizationContext {
  def Noop[X]: MemoizationContext[X] = new MemoizationContext[X] {
    def apply(memoId: Int)(implicit asyncContext: ExecutionContext) = Left(noopMemoizer)
  }
}

trait MemoizationComponent {
  type MemoContext[X] <: MemoizationContext[X]

  def memoizationContext[X]: MemoContext[X]
}

// vim: set ts=4 sw=4 et:
