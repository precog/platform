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

import com.precog.common._
import com.precog.util._
import com.precog.util.Bijection._

import blueeyes.json.JsonAST._
import blueeyes.json.Printer._

import java.math.BigInteger
import java.nio.ByteBuffer

import scala.annotation.tailrec
import scala.collection.generic.CanBuildFrom
import scala.math.BigDecimal


// TODO: optimize
package object leveldb {
  val UTF8 = java.nio.charset.Charset.forName("UTF-8")

  implicit object id2ab extends Bijection[Identities, Array[Byte]] {
    def apply(id : Identities) = id.foldLeft(ByteBuffer.allocate(8 * id.size))((b, id) => b.putLong(id)).array
    def unapply(ab : Array[Byte]) = {
      val buf = ByteBuffer.wrap(ab)
      @tailrec def read(acc: Vector[Long]): Vector[Long] = 
        if (buf.remaining >= 8) read(acc :+ buf.getLong)
        else                    acc

      read(Vector.empty[Long])
    }
  }

  implicit object booltoab extends Bijection[Boolean, Array[Byte]] {
    def apply(b : Boolean) = ByteBuffer.allocate(1).put(if (b) 0x1 else 0x0).array
    def unapply(ab : Array[Byte]) = ByteBuffer.wrap(ab).get != 0x0
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


  trait LengthEncodedArrayBijection[A] extends Bijection[A, Array[Byte]] {
    abstract override def apply(a : A) = {
      val bytes: Array[Byte] = super.apply(a)
      ByteBuffer.allocate(bytes.length + 4).putInt(bytes.length).put(bytes).array
    }

    abstract override def unapply(ab : Array[Byte]) = super.unapply(ab.drop(4))
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

  def idLen(length: Int) = Array[Byte]((length >> 8).asInstanceOf[Byte], (length & 0xff).asInstanceOf[Byte])

/*
  def projectionBijection(descriptor: ProjectionDescriptor): Bijection[SValue, Array[Byte]] = new Bijection[JValue, Array[Byte]] {
    def apply(jv: JValue) = {
      def lengthEncoded(valueType: SType) : Array[Byte] => Array[Byte] = {
        (a: Array[Byte]) => valueType match {
          case SDecimalArbitrary | SStringArbitrary => ByteBuffer.allocate(a.length + 4).putInt(a.length).put(a).array
          case SStringFixed(width)                  => ByteBuffer.allocate(width).put(a, 0, width).array
          case _ => a
        }
      } 

      val (len, arrays) = descriptor.columns.foldRight((0, List.empty[Array[Byte]])) {
        case (QualifiedSelector(path, selector, valueType), (len, acc)) =>
          val v = lengthEncoded(valueType) {
            jv(selector) match {
              case JBool(value)   => value.as[Array[Byte]]
              case JInt(value)    => value.bigInteger.toByteArray //TODO: Specialize to long if possible
              case JDouble(value) => value.as[Array[Byte]]
              case JString(value) => value.as[Array[Byte]] //TODO: Specialize for fixed length
              case JNothing       => Array[Byte]()
              case JNull          => Array[Byte]()
              case x              => sys.error("Column selector " + selector + " returns a non-leaf JSON value: " + compact(render(x)))
            }
          }

          (len + v.length, v :: acc)
      }

      arrays.foldLeft(ByteBuffer.allocate(len))((buf, arr) => buf.put(arr)).arr
    }

    def unapply(buf: Array[Byte]) = {
      def getColumnValue(valueType: SType[SValue]) = valueType match {
        case SLong => JInt(buf.getLong)
        case SDouble => JDouble(buf.getDouble)
        case SBoolean => JBool(buf.get != 0x0)
        case SNull => JNull
        case SNothing => JNothing
        case SDecimalArbitrary => //TODO: Specialize for fixed length
          val len = if (useColumnWidth) SDecimalArbitrary.width.getOrElse(buf.getInt) else buf.remaining
          val scale = buf.getInt
          val target = new Array[Byte](len - 4)
          buf.get(target)
          JInt(new BigInt(new BigInteger(target))) //TODO: Assume that we're storing BigInt as BigDecimal

        case SStringArbitrary => //TODO: Specialize for fixed length
          val len = if (useColumnWidth) SStringArbitrary.width.getOrElse(buf.getInt) else buf.remaining
          val target = new Array[Byte](len)
          buf.get(target)
          JString(new String(target, UTF8))

      }

      descriptor.columns match {
        case valueType :: Nil => getColumnValue(valueType.valueType)
        case types => types.foldLeft[JValue](JNothing) {
          case (obj, QualifiedSelector(path, selector, valueType)) => obj.set(selector, getColumnValue(valueType))
        }
      }
    }
  }
  */
}

