package com.precog.yggdrasil
package serialization

import com.precog.util._
import com.precog.common.VectorCase
import Bijection._

import java.math.BigInteger
import java.nio.ByteBuffer
import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom

package object bijections {
  private val UTF8 = java.nio.charset.Charset.forName("UTF-8")

  implicit object id2ab extends Bijection[Identities, Array[Byte]] {
    def apply(id : Identities) = id.foldLeft(ByteBuffer.allocate(8 * id.size))((b, id) => b.putLong(id)).array
    def unapply(ab : Array[Byte]) = {
      val buf = ByteBuffer.wrap(ab)
      @tailrec def read(acc: Vector[Long]): Vector[Long] = 
        if (buf.remaining >= 8) read(acc :+ buf.getLong)
        else                    acc

      VectorCase.fromSeq(read(Vector.empty[Long]))
    }
  }

  implicit object booltoab extends Bijection[Boolean, Array[Byte]] {
    def apply(b : Boolean) = ByteBuffer.allocate(1).put(if (b) (0x1: Byte) else (0x0: Byte)).array
    def unapply(ab : Array[Byte]) = ByteBuffer.wrap(ab).get != (0x0: Byte)
  }

  implicit object itoab extends Bijection[Int, Array[Byte]] {
    def apply(d : Int) = ByteBuffer.allocate(4).putInt(d).array
    def unapply(ab : Array[Byte]) = ByteBuffer.wrap(ab).getInt
  }

  implicit object ltoab extends Bijection[Long, Array[Byte]] {
    def apply(d : Long) = ByteBuffer.allocate(8).putLong(d).array
    def unapply(ab : Array[Byte]) = ByteBuffer.wrap(ab).getLong
  }

  implicit object d2ab extends Bijection[Double, Array[Byte]] {
    def apply(d : Double) = ByteBuffer.allocate(8).putDouble(d).array
    def unapply(ab : Array[Byte]) = ByteBuffer.wrap(ab).getDouble
  }

  implicit object s2ab extends Bijection[String, Array[Byte]] {
    def apply(s : String) = s.getBytes(UTF8)
    def unapply(ab : Array[Byte]) = new String(ab, UTF8)
  }

  implicit object bi2ab extends Bijection[BigInteger, Array[Byte]] {
    def apply(bi : BigInteger) = bi.toByteArray
    def unapply(ab : Array[Byte]) = new BigInteger(ab)
  }

  implicit object bd2ab extends Bijection[BigDecimal, Array[Byte]] {
    def apply(bd: BigDecimal) = bd.bigDecimal.scale.as[Array[Byte]] ++ bd.bigDecimal.unscaledValue.toByteArray
    def unapply(ab: Array[Byte]) = new BigDecimal(new java.math.BigDecimal(new BigInteger(ab.drop(4)), ab.take(4).as[Int]))
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
}

// vim: set ts=4 sw=4 et:
