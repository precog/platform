package com.precog
package daze

import yggdrasil._
import yggdrasil.serialization._
import memoization._

trait TableOps {
  implicit def extend(t: Table): TableExtensions

  def empty(idCount: Int): Table

  def singleton[@specialized(Boolean, Int, Long, Float, Double) A](ctype: CType { type CA = A }, value: A): Table
}

trait TableExtensions {
  type Memoable[α] = Table

  def value: Table

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join(t: Table, sharedPrefixLength: Int)(f: BinaryOpSet): Table

  // concatenate identities
  def crossLeft(t: Table)(f: BinaryOpSet): Table

  // concatenate identities
  def crossRight(t: Table)(f: BinaryOpSet): Table

  // pad identities to the longest side, then sort -u by all identities
  def paddedMerge(t: Table, nextId: () => Identity): Table

  // merge sorted uniq by identities and values. Input datasets must have equal identity counts
  def union(t: Table, memoCtx: MemoizationContext[Memoable])(implicit ss: TableSerialization): Table

  // inputs are sorted in identity order - merge by identity, sorting any runs of equal identities
  // using the value ordering, equal identity, equal value are the only events that persist
  // Input datasets must have equal identity counts
  def intersect(d2: Dataset[A], memoCtx: MemoizationContext[Memoable])(implicit ord: Order[A], ss: SortSerialization[IA]): Dataset[A] 

  def map[B](f: A => B): Dataset[B] 

  def collect[B](pf: PartialFunction[A, B]): Dataset[B]

  def reduce[B](base: B)(f: (B, A) => B): B

  def count: BigInt

  //uniq by value, assign new identities
  def uniq(nextId: () => Identity, memoId: Int, ctx: MemoizationContext[Memoable])(implicit buffering: Buffering[A], fs: SortSerialization[A]): Dataset[A] 

  // identify(None) strips all identities
  def identify(nextId: Option[() => Identity]): Dataset[A]

  // reorders identities such that the prefix is in the order of the vector of indices supplied, and the order of
  // the remaining identities is unchanged (but the ids are retained as a suffix) then sort by identity
  def sortByIndexedIds(indices: Vector[Int], memoId: Int, memoCtx: MemoizationContext[Memoable])(implicit fs: SortSerialization[IA]): Dataset[A] 
  
  def memoize(memoId: Int, memoCtx: MemoizationContext[Memoable])(implicit serialization: IncrementalSerialization[(Identities, A)]): Dataset[A] 

}


// vim: set ts=4 sw=4 et:
