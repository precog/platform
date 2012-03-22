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
package daze

import yggdrasil.FileSerialization
import scalaz._
import scalaz.effect._

abstract class CogroupF[A, B, C](final val join: Boolean) {
  def left(a: A): C
  def both(a: A, b: B): C
  def right(b: B): C
}

trait DatasetOps[Dataset[_]] {
  implicit def extend[A](d: Dataset[A]): DatasetExtensions[Dataset, A]

  def empty[A]: Dataset[A] 

  def point[A](value: A): Dataset[A] 

  def flattenSorted[A](d: Dataset[Dataset[A]], idPrefixLength: Int, memoId: Int): Dataset[A]
}

trait DatasetExtensions[Dataset[_], A] {
  def value: Dataset[A]

  // join must drop a prefix of identities from d2 up to the shared prefix length
  def join[B, C](d2: Dataset[B], sharedPrefixLength: Int)(f: PartialFunction[(A, B), C]): Dataset[C]

  def crossLeft[B, C](d2: Dataset[B])(f: PartialFunction[(A, B), C]): Dataset[C] 

  def crossRight[B, C](d2: Dataset[B])(f: PartialFunction[(A, B), C]): Dataset[C] 

  // pad identities to the longest side on identity union
  // value union discards identities
  def union(d2: Dataset[A], idUnion: Boolean): Dataset[A]

  // value intersection discards identities
  def intersect(d2: Dataset[A], idIntersect: Boolean): Dataset[A]

  //def merge(d2: Dataset[A])(implicit order: Order[A]): Dataset[A]

  def map[B](f: A => B): Dataset[B] 

  def flatMap[B](f: A => Dataset[B]): Dataset[B]

  def collect[B](pf: PartialFunction[A, B]): Dataset[B]

  def reduce[B](base: B)(f: (B, A) => B): B

  def count: BigInt

  def uniq: Dataset[A]

  // identify(None) strips all identities
  def identify(baseId: Option[() => Long]): Dataset[A]

  def sortByIds(memoId: Int)(cm: Manifest[A], fs: FileSerialization[A]): Dataset[A]
  def sortByIndexedIds(indices: Vector[Int], memoId: Int)(implicit cm: Manifest[A], fs: FileSerialization[A]): Dataset[A]

  def sortByValue(memoId: Int)(implicit order: Order[A], cm: Manifest[A], fs: FileSerialization[A]): Dataset[A] 
  
  def memoize(memoId: Int)(implicit fs: FileSerialization[A]): Dataset[A] 

  def group[K](memoId: Int)(keyFor: A => K)(implicit ord: Order[K], fs: FileSerialization[A], kvs: FileSerialization[(K, Dataset[A])]): Dataset[(K, Dataset[A])]

  def perform(io: IO[_]): Dataset[A]
}





// vim: set ts=4 sw=4 et:
