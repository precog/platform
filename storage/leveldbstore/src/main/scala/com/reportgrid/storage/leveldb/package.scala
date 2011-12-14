package com.reportgrid.storage
import java.math._
import scala.collection.generic.CanBuildFrom

import leveldb.Bijection._

// TODO: optimize
package object leveldb {
  implicit object itoab extends Bijection[Int, Array[Byte]] {
    def apply(i: Int) = List(24,16,8,0).map{ shift => (i >> shift).asInstanceOf[Byte] }.toArray
    def unapply(ab: Array[Byte]) = List(24,16,8,0).zip(ab.take(4)).map{ case (shift,b) => (b & 0xff) << shift }.foldLeft(0)(_ | _) 
  }

  implicit object ltoab extends Bijection[Long, Array[Byte]] {
    def apply(l : Long) = Array[Byte]((l >> 56).asInstanceOf[Byte],
				      (l >> 48).asInstanceOf[Byte],
				      (l >> 40).asInstanceOf[Byte],
				      (l >> 32).asInstanceOf[Byte],
				      (l >> 24).asInstanceOf[Byte],
				      (l >> 16).asInstanceOf[Byte],
				      (l >> 8).asInstanceOf[Byte],
				      l.asInstanceOf[Byte])
    def unapply(ab : Array[Byte]) = (ab(0) & 0xffl) << 56 |
				    (ab(1) & 0xffl) << 48 |
				    (ab(2) & 0xffl) << 40 |
				    (ab(3) & 0xffl) << 32 |
				    (ab(4) & 0xffl) << 24 |
				    (ab(5) & 0xffl) << 16 |
				    (ab(6) & 0xffl) << 8 |
				    (ab(7) & 0xffl)
  }
	  
  implicit object bi2ab extends Bijection[BigInteger, Array[Byte]] {
    def apply(bi : BigInteger) = bi.toByteArray
    def unapply(ab : Array[Byte]) = new BigInteger(ab)
  }

  implicit object bd2ab extends Bijection[BigDecimal, Array[Byte]] {
    def apply(bd: BigDecimal) = bd.scale.as[Array[Byte]] ++ bd.unscaledValue.as[Array[Byte]]
    def unapply(ab: Array[Byte]) = new BigDecimal(ab.drop(4).as[BigInteger], ab.take(4).as[Int])
  }

  implicit def l2ab[M[X] <: Traversable[X], T](implicit cbf: CanBuildFrom[Stream[T], T, M[T]], bij: Bijection[T, Array[Byte]]): Bijection[M[T], Array[Byte]] = new Bijection[M[T], Array[Byte]] {
    def apply(l: M[T]) = l.map(_.as[Array[Byte]]).foldLeft(Array[Byte]()) { (a, b) => a ++ b.length.as[Array[Byte]] ++ b }
    def unapply(ab: Array[Byte]) = {
      def _unapply(offset: Int): Stream[T] = {
        if (offset >= ab.length) Stream.empty[T]
        else {
          val len = ab.slice(offset, offset + 4).as[Int]
          ab.slice(offset + 4, offset + len + 4).as[T] +: _unapply(offset + len + 4)
        }
      }

      _unapply(0).map(identity[T])
    }
  }

  def idLen(length: Int) = Array[Byte]((length >> 8).asInstanceOf[Byte], (length & 0xff).asInstanceOf[Byte])
}

