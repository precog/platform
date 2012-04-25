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
  type Memoable[E]
  type MemoContext <: MemoizationContext[Memoable]

  def withMemoizationContext[A](f: MemoContext => A): A
}

trait MemoCache {
  def expire(memoId: MemoId): Unit
  def purge(): Unit
}

object MemoCache {
  object Noop extends MemoCache {
    def expire(memoId: Int) = ()
    def purge() = ()
  }
}

trait MemoizationContext[Memoable[_]] {
  def cache: MemoCache
  def memoize[A](dataset: Memoable[A], memoId: Int)(implicit serialization: IncrementalSerialization[A]): Memoable[A]
  def sort[A](values: Memoable[A], memoId: Int)(implicit buffering: Buffering[A], fs: SortSerialization[A]): Memoable[A]
}
