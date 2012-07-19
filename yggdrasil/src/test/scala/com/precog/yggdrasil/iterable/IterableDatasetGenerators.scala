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
package com.precog.yggdrasil
package iterable

import memoization._
import serialization._
import com.precog.common._
import com.precog.util.IdGen
import com.precog.util.IOUtils

import blueeyes.util.Clock
import akka.actor.ActorSystem
import akka.dispatch.ExecutionContext

import java.io.{DataInputStream, DataOutputStream, File}

import scala.annotation.tailrec
import scalaz.Ordering._

import scalaz.{NonEmptyList => NEL, _}
import scalaz.effect._
import scalaz.iteratee._
import scalaz.std.anyVal._
import scalaz.std.tuple._
import Iteratee._
import Either3._

import org.specs2.mutable._
import org.specs2.ScalaCheck
import org.scalacheck.Gen
import org.scalacheck.Gen._
import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._

trait IterableDatasetGenerators {
  type Record[A] = (Identities, A)
  type DSPair[A] = Pair[IterableDataset[A], IterableDataset[A]]

  case class MergeSample(maxIds: Int, l1: IterableDataset[Long], l2: IterableDataset[Long])

  case class IdCount(idCount: Int)

  implicit val mergeSampleArbitrary = Arbitrary(
    for {
      i1 <- choose(0, 3) map (IdCount(_: Int))
      i2 <- choose(1, 3) map (IdCount(_: Int)) if i2 != i1
      l1 <- dsGen[Long]()(Gen.value(i1), genLong)
      l2 <- dsGen[Long]()(Gen.value(i2), genLong)
    } yield {
      MergeSample(i1.idCount max i2.idCount, l1, l2)
    }
  )

  implicit val genLong: Gen[Long] = arbLong.arbitrary

  val genVariableIdCount = Gen.choose(0, 10).map(IdCount(_))

  def genFixedIdCount(count: Int): Gen[IdCount] = Gen.value(IdCount(count))

  implicit def recGen[A](idCount: IdCount, agen: Gen[A]): Gen[Record[A]] = 
    for {
      ids   <- listOfN(idCount.idCount, arbitrary[Long])
      value <- agen
    } yield {
      (VectorCase(ids: _*), value)
    }

  implicit def dsGen[A](minSize: Int = 0)(implicit count: Gen[IdCount], agen: Gen[A]): Gen[IterableDataset[A]] = {
    for {
      idCount  <- count
      recCount <- Gen.choose(minSize, 100)
      l        <- listOfN(recCount, recGen(idCount, agen))
    } yield IterableDataset(idCount.idCount, l.distinct)
  }

  implicit def equalCountDSPair[A](implicit count: Gen[IdCount], agen: Gen[A]): Gen[DSPair[A]] = {
    for {
      idCount <- count
      l       <- listOf(recGen(idCount, agen))
      m       <- listOf(recGen(idCount, agen))
    } yield (IterableDataset(idCount.idCount, l.distinct), IterableDataset(idCount.idCount, m.distinct))
  }

  implicit def groupingGen[K,A](minSize: Int = 0)(implicit groupingFunc: A => K, count: Gen[IdCount], agen: Gen[A], orderA: Order[A], orderK: Order[K]): Gen[IterableGrouping[K,IterableDataset[A]]] = {
    implicit val orderingA = tupledIdentitiesOrder[A](IdentitiesOrder).toScalaOrdering
    implicit val orderingK = orderK.toScalaOrdering

    for {
      ds <- dsGen[A](minSize)
    } yield {
      IterableGrouping(ds.iterable.groupBy { case (k,v) => groupingFunc(v) }.map { case (k,v) => (k, IterableDataset[A](ds.idCount, List(v.toSeq: _*).sorted)) }.toList.sortBy(_._1).toIterator)
    }
  }

  implicit def groupingNelGen[K,A](implicit groupingFunc: A => K, count: Gen[IdCount], agen: Gen[A], orderA: Order[A], orderK: Order[K]): Gen[IterableGrouping[K,NEL[IterableDataset[A]]]] = {
    groupingGen[K,A](1).map {
      g => {
        IterableGrouping(g.iterator.map { 
          case (k,IterableDataset(count,iterable)) => {
            val splitList = iterable.toList.grouped(1).map(IterableDataset(count,_)).toList
            (k, NEL.nel(splitList.head, splitList.tail))
          } 
        })
      }
    }
  }

  implicit def arbLongDataset(implicit idCount: Gen[IdCount]) = Arbitrary(dsGen[Long]())

  implicit def arbLongDSPair(implicit idCount: Gen[IdCount]) = Arbitrary(equalCountDSPair[Long])
}

// vim: set ts=4 sw=4 et:
