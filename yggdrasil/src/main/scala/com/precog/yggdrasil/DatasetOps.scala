/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog
package yggdrasil

import serialization._
import memoization._

trait DatasetOps[Dataset[_], Memoable[_], Grouping[_, _]] {
  implicit def extend[A](d: Dataset[A]): DatasetExtensions[Dataset, Memoable, Grouping, A]

  def empty[A](idCount: Int): Dataset[A] 

  def point[A](value: A): Dataset[A] 

  // used for load - concatenate inner datasets and assign new identities
  // ordering is irrelevant since the new identities will be in ascending order
  def flattenAndIdentify[A](d: Dataset[Dataset[A]], nextId: () => Identity): Dataset[A]
}
  
// groups have no identities
trait GroupingOps[Dataset[_], Memoable[_], Grouping[_, _]] {
  // if isUnion, cogroup, merging datasets of the common key by the union operation
  // if !isUnion (intersect) retain where keys are equivalent, merging the inner datasets using the intersect operation
  // keep result in key order
  def mergeGroups[A, K](d1: Grouping[K, Dataset[A]], d2: Grouping[K, Dataset[A]], isUnion: Boolean, memoCtx: MemoizationContext[Memoable])(implicit ord1: Order[A], ord: Order[K], ss: SortSerialization[(Identities, A)]): Grouping[K, Dataset[A]] 

  // intersect by key, concatenating the NELs
  // keep result in key order
  def zipGroups[A, K: Order](d1: Grouping[K, NEL[Dataset[A]]], d2: Grouping[K, NEL[Dataset[A]]]): Grouping[K, NEL[Dataset[A]]]

  // the resulting Dataset[B] needs to be merged such that it is value-unique and has new identities
  def flattenGroup[A, K, B](g: Grouping[K, NEL[Dataset[A]]], nextId: () => Identity, memoId: Int, memoCtx: MemoizationContext[Memoable])(f: (K, NEL[Dataset[A]]) => Dataset[B])(implicit buffering: Buffering[B], fs: IncrementalSerialization[(Identities, B)]): Dataset[B]

  def mapGrouping[K, A, B](g: Grouping[K, A])(f: A => B): Grouping[K, B]
}

trait CogroupF[A, B, C] {
  def left(a: A): C
  def both(a: A, b: B): C
  def right(b: B): C
}

trait DatasetExtensions[Dataset[_], Memoable[_], Grouping[_, _], A] {
  type IA = (Identities, A)

  def value: Dataset[A]

  // Input datasets must have equal identity counts, and must be sorted on identity
  def cogroup[B, C](d2: Dataset[B])(f: CogroupF[A, B, C]): Dataset[C]

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join[B, C](d2: Dataset[B], sharedPrefixLength: Int)(f: PartialFunction[(A, B), C]): Dataset[C]

  // concatenate identities
  def crossLeft[B, C](d2: Dataset[B])(f: PartialFunction[(A, B), C]): Dataset[C] 

  // concatenate identities
  def crossRight[B, C](d2: Dataset[B])(f: PartialFunction[(A, B), C]): Dataset[C] 

  // pad identities to the longest side, then sort -u by all identities
  def paddedMerge(d2: Dataset[A], nextId: () => Identity): Dataset[A]

  // merge sorted uniq by identities and values. Input datasets must have equal identity counts
  def union(d2: Dataset[A], memoCtx: MemoizationContext[Memoable])(implicit ord: Order[A], ss: SortSerialization[IA]): Dataset[A]

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

  // for each value, calculate the keys for that value - this should be as singleton dataset
  // sort by key then by the identity ordering of the input dataset
  def group[K](memoId: Int, memoCtx: MemoizationContext[Memoable])(keyFor: A => Dataset[K])(implicit ord: Order[K], kvs: SortSerialization[(K, Identities, A)], ms: IncrementalSerialization[(Identities, A)]): Grouping[K, Dataset[A]]
}

// vim: set ts=4 sw=4 et:
