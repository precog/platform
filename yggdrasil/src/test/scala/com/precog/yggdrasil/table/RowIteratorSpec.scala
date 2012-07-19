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

/*
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
      //len <- choose(1, 1000)
      len <- choose(1, 20)
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
              else {
                // Possible cartesian needed, find the full equal sequences on the right and left
                val (l1Seq, l1Rem) = l1.partition(_._1 == id1)
                val (l2Seq, l2Rem) = l2.partition(_._1 == id2)

                rec(l1Rem, l2Rem, l1Seq.flatMap { case (id1, v1) => l2Seq.map { case (id2, v2) => (id1, v1, v2) } }.reverse ::: r)
              }
            case Nil => r.reverse
          }
        case Nil => r.reverse
      }
    }

    rec(l1, l2, Nil)
  }

  "a JoinRowIterator" should {
//    "obtain the same results as a strict join" in {
//      check { (i1: LongBufferRowIterator, i2: LongBufferRowIterator) => 
//        val expected = joinLists(i1.keys.array.toList zip i1.values.array.toList, i2.keys.array.toList zip i2.values.array.toList)
//        forall(JoinRowIterator(i1, i2, 1)) { iter =>
//
//        //println("==============")
//        //println("i1 = " + (i1.keys.array.toList zip i1.values.array.toList))
//        //println("i2 = " + (i2.keys.array.toList zip i2.values.array.toList))
//        //println("expected = " + expected)
//
//          var joined: List[(Long, Long, Long)] = (iter.idAt(0), iter.longAt(0), iter.longAt(1)) :: Nil
//          while (iter.advance(1)) joined = (iter.idAt(0), iter.longAt(0), iter.longAt(1)) :: joined
//          joined.reverse must_== expected
//        }
//      }
//    }

    "succeed on failing sample" in {
      val left = List((-4611686018427387904l,-4611686018427387904l),
                      (-4611686018427387904l,-2705084260014779340l),
                      (-2044762096336814645l, 2958928930257704843l),
                      (-1341226650521502488l,-2659372784769374987l),
                      (-867584788221027215l,4611686018427387903l),
                      (-1l,-1800813238494576921l),
                      (0l,4611686018427387903l),
                      (1l,1142849289501654602l),
                      (755301456235533030l,-1570058678012065166l),
                      (2091921826755807125l,65924511511850270l),
                      (4611686018427387903l,554028959602010689l),
                      (4611686018427387903l,4088835802276197721l))
      val (leftKeys, leftValues) = left.unzip

      val right = List( (-4611686018427387904l,962540448693647226l),
                        (-4611686018427387904l,1l),
                        (-4455741525559278285l,-1l),
                        (-2305701152332638002l,3882370639322964133l),
                        (-1625457804994147303l,2883993262929241677l),
                        (-112171687244001899l,1l),
                        (-1l,0l),
                        (0l,2561183432787557256l),
                        (0l,-1l),
                        (0l,2237921431685816929l),
                        (1l,4385342101506736224l),
                        (552644284652484229l,2759072634162894520l),
                        (3102319900518874422l,-4611686018427387904l),
                        (3341413993240482911l,4611686018427387903l),
                        (4611686018427387903l,1l),
                        (4611686018427387903l,-4144617194929742461l))
       val (rightKeys, rightValues) = right.unzip

      val expected = joinLists(left, right)
      println(expected.mkString("\n"))
      val leftIter = LongBufferRowIterator(longBuffer(leftKeys), longBuffer(leftValues), DynCPath(0))
      val rightIter = LongBufferRowIterator(longBuffer(rightKeys), longBuffer(rightValues), DynCPath(1))

      forall(JoinRowIterator(leftIter, rightIter, 1)) { iter =>
        var joined: List[(Long, Long, Long)] = (iter.idAt(0), iter.longAt(0), iter.longAt(1)) :: Nil
        while (iter.advance(1)) joined = (iter.idAt(0), iter.longAt(0), iter.longAt(1)) :: joined

        joined.reverse must_== expected
      }
    }
  }
}
    */

// vim: set ts=4 sw=4 et:
