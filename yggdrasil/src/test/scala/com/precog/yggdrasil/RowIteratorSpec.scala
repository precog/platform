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
      //len <- choose(1, 1000)
      len <- choose(1, 3)
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
    "obtain the same results as a strict join" in {
//      val k1 = List[Long](0, 1, 4, 4, 6, 7, 7)
//      val v1 = List[Long](12, 13, 10, 15, 8, 2, 3)
//      val k2 = List[Long](1, 2, 2, 4, 4, 5, 7)
//      val v2 = List[Long](10, 22, 26, 1, 30, 5, 4)
//
//      println(joinLists(k1 zip v1, k2 zip v2))
//
//      val iter1 = LongBufferRowIterator(LongBuffer.allocate(k1.length).put(k1.toArray).clear().asInstanceOf[LongBuffer], LongBuffer.allocate(v1.length).put(v1.toArray).clear().asInstanceOf[LongBuffer], DynCPath(0))
//      val iter2 = LongBufferRowIterator(LongBuffer.allocate(k2.length).put(k2.toArray).clear().asInstanceOf[LongBuffer], LongBuffer.allocate(v2.length).put(v2.toArray).clear().asInstanceOf[LongBuffer], DynCPath(1))
//
//      val jIter = JoinRowIterator(iter1, iter2, 1)
//
//      do {
//        println((jIter.idAt(0), jIter.longAt(0), jIter.longAt(1)))
//      } while (jIter.advance(1))
//
//      12 must_== 12
      check { (i1: LongBufferRowIterator, i2: LongBufferRowIterator) => 
        val expected = joinLists(i1.keys.array.toList zip i1.values.array.toList, i2.keys.array.toList zip i2.values.array.toList)
        val iter = JoinRowIterator(i1, i2, 1)

        println("==============")
        println("i1 = " + (i1.keys.array.toList zip i1.values.array.toList))
        println("i2 = " + (i2.keys.array.toList zip i2.values.array.toList))
        println("expected = " + expected)

        var initial = true
        var doneAdvancing = false

        foreach(expected) {
          case (id, lv, rv) => 
            doneAdvancing must_!= true
            if (!initial) { doneAdvancing = iter.advance(1) }
            initial = false
            println((id, lv, rv) + " :: " + (iter.idAt(0), iter.longAt(0), iter.longAt(1)))
            (iter.idAt(0) must_== id) and (iter.longAt(0) must_== lv) and (iter.longAt(1) must_== rv)
        }
      }
    }
  }
}

// vim: set ts=4 sw=4 et:
