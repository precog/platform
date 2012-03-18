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

import java.nio._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Gen._

trait RowIteratorSpec {
  type IterBuilder[B <: Buffer, RI <: BufferRowIterator[B]] = (LongBuffer, B, CPath) => RI
  def longBuffer(longs: List[Long]) = LongBuffer.allocate(longs.length).put(longs.toArray).clear().asInstanceOf[LongBuffer]

  implicit val arbByteBuffer: Int => Gen[ByteBuffer] = (len: Int) => for {
    bytes <- listOfN(len, Gen.oneOf(0x00: Byte, 0x01: Byte))
  } yield {
    ByteBuffer.allocate(bytes.length).put(bytes.toArray).clear().asInstanceOf[ByteBuffer]
  }

  implicit val arbIntBuffer: Int => Gen[IntBuffer] = (len: Int) => for {
    ints  <- listOfN(len, arbitrary[Int])
  } yield {
    IntBuffer.allocate(ints.length).put(ints.toArray).clear().asInstanceOf[IntBuffer]
  }

  implicit val arbLongBuffer: Int => Gen[LongBuffer] = (len: Int) => for {
    longs <- listOfN(len, arbitrary[Long])
  } yield longBuffer(longs)
  
  implicit val arbCPath: Arbitrary[CPath] = Arbitrary(for (l <- arbitrary[Long]) yield DynCPath(l))

  def genBufIterator[B <: Buffer, RI <: BufferRowIterator[B]](f: IterBuilder[B, RI])(implicit bufGen: Int => Gen[B]): Gen[RI] = 
    for {
      len <- choose(1, 1000)
      ids <- listOfN(len, arbitrary[Long])
      buf <- bufGen(len)
      sel <- arbitrary[CPath]
    } yield f(longBuffer(ids.sorted), buf, sel)

  implicit val boolBufferIterBuilder = Arbitrary(genBufIterator[ByteBuffer, BoolBufferRowIterator](BoolBufferRowIterator.apply _))
  implicit val intBufferIterBuilder  = Arbitrary(genBufIterator[IntBuffer, IntBufferRowIterator](IntBufferRowIterator.apply _))
  implicit val longBufferIterBuilder = Arbitrary(genBufIterator[LongBuffer, LongBufferRowIterator](LongBufferRowIterator.apply _))
}

object BufferRowIteratorSpec extends Specification with ScalaCheck with RowIteratorSpec {
  "a BoolBufferRowIterator" should {
    "properly advance based on a count" in {
      check { (iter: BoolBufferRowIterator) => 
        forall(choose(0, iter.remaining - 1).sample) { i => 
          iter.advance(i); 
          iter.boolAt(0) must_== (if (iter.values.get(i) == (0x00: Byte)) false else true)
        }
      }
    }
  }

  "a IntBufferRowIterator" should {
    "properly advance based on a count" in {
      check { (iter: IntBufferRowIterator) => 
        forall(choose(0, iter.remaining - 1).sample) { i => 
          iter.advance(i); 
          iter.intAt(0) must_== iter.values.get(i) 
        }
      }
    }
  }

  "a LongBufferRowIterator" should {
    "properly advance based on a count" in {
      check { (iter: LongBufferRowIterator) =>
        forall(choose(0, iter.remaining - 1).sample) { i => 
          iter.advance(i); 
          iter.longAt(0) must_== iter.values.get(i) 
        }
      }
    }
  }
}

object JoinRowIteratorSpec extends Specification with ScalaCheck with RowIteratorSpec {
  import scala.annotation.tailrec  

  def joinLists(l1: List[(Long, Long)], l2: List[(Long, Long)]): List[(Long, Long, Long)] = {
    @tailrec def rec(l1: List[(Long, Long)], l2: List[(Long, Long)], r: List[(Long, Long, Long)]): List[(Long, Long, Long)] = {
      l1 match {
        case (id1, v1) :: xs =>
          l2 match {
            case (id2, v2) :: ys =>
              if (id1 < id2) rec(xs, l2, r)
              else if (id1 > id2) rec(l1, ys, r) 
              else rec(xs, ys, (id1, v1, v2) :: r)

            case Nil => r
          }
        case Nil => r
      }
    }

    rec(l1, l2, Nil)
  }

  "a JoinRowIterator" should {
    "obtain the same results as a strict join" in {
      check { (i1: LongBufferRowIterator, i2: LongBufferRowIterator) => 
        val expected = joinLists(i1.keys.array.toList zip i1.values.array.toList, i2.keys.array.toList zip i2.values.array.toList).reverse
        val iter = JoinRowIterator(i1, i2, 1)

        var initial = true
        foreach(expected) {
          case (id, lv, rv) => 
            if (!initial) iter.advance(1)
            initial = false
            println((id, lv, rv) + " :: " + (iter.idAt(0), iter.longAt(0), iter.longAt(1)))
            (iter.idAt(0) must_== id) and (iter.longAt(0) must_== lv) and (iter.longAt(1) must_== rv)
        }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
