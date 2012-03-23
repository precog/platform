package com.precog
package daze

import yggdrasil.FileSerialization
import scalaz._
import scalaz.effect._

trait DatasetOps[Dataset[_], Grouping[_, _]] {
  implicit def extend[A](d: Dataset[A]): DatasetExtensions[Dataset, A]

  def empty[A]: Dataset[A] 

  def point[A](value: A): Dataset[A] 

  // used for load - concatenate inner datasets and assign new identities
  def flattenAndIdentify[A](d: Dataset[Dataset[A]], nextId: => Long, memoId: Int): Dataset[A]
}
  
// groups have no identities
trait GroupingOps[Dataset[_], Grouping[_, _]] {
  // if isUnion, cogroup, merging datasets of the common key by the union operation
  // if !isUnion (intersect) retain where keys are equivalent, merging the inner datasets using the intersect operation
  // keep result in key order
  def mergeGroups[A: Order, K: Order](d1: Grouping[K, Dataset[A]], d2: Grouping[K, Dataset[A]], isUnion: Boolean): Grouping[K, Dataset[A]]

  // intersect by key, concatenating the NELs
  // keep result in key order
  def zipGroups[A, K: Order](d1: Grouping[K, NEL[Dataset[A]]], d2: Grouping[K, NEL[Dataset[A]]]): Grouping[K, NEL[Dataset[A]]]

  // the resulting Dataset[B] needs to be merged such that it is value-unique and has new identities
  def flattenGroup[A, K, B: Order](g: Grouping[K, NEL[Dataset[A]]], nextId: => Long)(f: (K, NEL[Dataset[A]]) => Dataset[B]): Dataset[B]

  def mapGrouping[K, A, B](g: Grouping[K, A])(f: A => B): Grouping[K, B]
}

trait DatasetExtensions[Dataset[_], Grouping[_, _], A] {
  def value: Dataset[A]

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join[B, C](d2: Dataset[B], sharedPrefixLength: Int)(f: PartialFunction[(A, B), C]): Dataset[C]

  // concatenate identities
  def crossLeft[B, C](d2: Dataset[B])(f: PartialFunction[(A, B), C]): Dataset[C] 

  // concatenate identities
  def crossRight[B, C](d2: Dataset[B])(f: PartialFunction[(A, B), C]): Dataset[C] 

  // pad identities to the longest side, then sort -u by identities
  def paddedMerge(d2: Dataset[A], nextId: => Long): Dataset[A]

  // merge sorted uniq by identities and values
  def union(d2: Dataset[A])(implicit order: Order[A]): Dataset[A]

  // inputs are sorted in identity order - merge by identity, sorting any runs of equal identities
  // using the value ordering, equal identity, equal value are the only events that persist
  def intersect(d2: Dataset[A])(implicit order: Order[A]): Dataset[A]

  def map[B](f: A => B): Dataset[B] 

  def collect[B](pf: PartialFunction[A, B]): Dataset[B]

  def reduce[B](base: B)(f: (B, A) => B): B

  def count: BigInt

  //uniq by value, assign new identities
  def uniq(nextId: => Long)(implicit order: Order[A]): Dataset[A]

  // identify(None) strips all identities
  def identify(nextId: Option[() => Long]): Dataset[A]

  // reorders identities such that the prefix is in the order of the vector of indices supplied, and the order of
  // the remaining identities is unchanged (but the ids are retained as a suffix) then sort by identity
  def sortByIndexedIds(indices: Vector[Int], memoId: Int)(implicit cm: Manifest[A], fs: FileSerialization[A]): Dataset[A]
  
  def memoize(memoId: Int)(implicit fs: FileSerialization[A]): Dataset[A] 

  def group[K](memoId: Int)(keyFor: A => K)(implicit ord: Order[K], fs: FileSerialization[A], kvs: FileSerialization[(K, Dataset[A])]): Grouping[K, Dataset[A]]

  def perform(io: IO[_]): Dataset[A]
}





// vim: set ts=4 sw=4 et:
