package com.precog.yggdrasil
package table

import serialization._
import memoization._
import com.precog.common.Path

trait TableOps {
  implicit def extend(t: Table): TableExtensions

  def empty(idCount: Int): Table

  def singleton[@specialized(Boolean, Long, Double) A](ctype: CType { type CA = A }, value: A): Table

  def fullProjection(userUID: String, path: Path, expiresAt: Long): Table

  def mask(userUID: String, path: Path): DatasetMask[Table]
}

trait TableExtensions {
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
  def union(t: Table, memoCtx: MemoizationContext[Iterable]): Table

  // inputs are sorted in identity order - merge by identity, sorting any runs of equal identities
  // using the value ordering, equal identity, equal value are the only events that persist
  // Input datasets must have equal identity counts
  def intersect(d2: Table, memoCtx: MemoizationContext[Iterable]): Table

  def collect(f: F1P[_, _]): Table

  def reduce[B](base: B)(f: F2P[B, _, B]): B

  def count: BigInt

  //uniq by value, assign new identities
  def uniq(nextId: () => Identity, memoId: Int, ctx: MemoizationContext[Iterable]): Table

  // identify(None) strips all identities
  def identify(nextId: Option[() => Identity]): Table

  // reorders identities such that the prefix is in the order of the vector of indices supplied, and the order of
  // the remaining identities is unchanged (but the ids are retained as a suffix) then sort by identity
  def sortByIndexedIds(indices: Vector[Int], memoId: Int, memoCtx: MemoizationContext[Iterable]): Table
  
  def memoize(memoId: Int, memoCtx: MemoizationContext[Iterable]): Table

}


// vim: set ts=4 sw=4 et:
