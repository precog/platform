package com.precog.yggdrasil

import java.nio._

import org.specs2.mutable.Specification
import org.specs2.ScalaCheck

import org.scalacheck.Arbitrary
import org.scalacheck.Arbitrary._
import org.scalacheck.Gen
import org.scalacheck.Gen._

object BufferRowIteratorSpec extends Specification with ScalaCheck {
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
// vim: set ts=4 sw=4 et:
