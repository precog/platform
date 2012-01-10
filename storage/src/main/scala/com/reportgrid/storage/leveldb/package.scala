package com.reportgrid.storage

import com.reportgrid.common._
import blueeyes.json.JsonAST._
import blueeyes.json.Printer._

import java.math._
import java.nio.ByteBuffer

import scala.collection.generic.CanBuildFrom

import leveldb.Bijection._

import com.reportgrid.common._

// TODO: optimize
package object leveldb {
  val UTF8 = java.nio.charset.Charset.forName("UTF-8")

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
    def apply(bd: BigDecimal) = bd.scale.as[Array[Byte]] ++ bd.unscaledValue.toByteArray
    def unapply(ab: Array[Byte]) = new BigDecimal(new BigInteger(ab.drop(4)), ab.take(4).as[Int])
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

  implicit object bb2ab extends Bijection[ByteBuffer, Array[Byte]] {
    def apply(bb: ByteBuffer) = {
      val result = new Array[Byte](bb.remaining)
      bb.get(result)
      result
    }

    def unapply(ab: Array[Byte]) = ByteBuffer.wrap(ab)
  }

  implicit object bb2l extends Bijection[ByteBuffer, Long] {
    def apply(bb : ByteBuffer) = bb.getLong
    def unapply(l : Long) = ByteBuffer.allocate(8).putLong(l).flip.asInstanceOf[ByteBuffer] //flip returns Buffer
  }

  implicit object bb2d extends Bijection[ByteBuffer, Double] {
    def apply(bb : ByteBuffer) = bb.getDouble
    def unapply(d : Double) = ByteBuffer.allocate(8).putDouble(d).flip.asInstanceOf[ByteBuffer] //flip returns Buffer
  }

  def projectionBijection2(descriptor: ProjectionDescriptor): Bijection[Seq[JValue], ByteBuffer] = new Bijection[Seq[JValue], ByteBuffer] {
    def useColumnWidth: Boolean = true
    
    def apply(values: Seq[JValue]) = {

      def lengthEncoded(valueType: PrimitiveType) : Array[Byte] => Array[Byte] = {
        (a: Array[Byte]) => valueType match {
          case ValueType.BigDecimal(None)    if useColumnWidth => ByteBuffer.allocate(a.length + 4).putInt(a.length).put(a).array
          case ValueType.BigDecimal(Some(l)) if useColumnWidth => sys.error("BigDecimal may not be used fixed-length yet.")
          case ValueType.String(None)        if useColumnWidth => ByteBuffer.allocate(a.length + 4).putInt(a.length).put(a).array
          case ValueType.String(Some(l))     if useColumnWidth => ByteBuffer.allocate(l).put(a, 0, l).array
          case _ => a
        }
      } 
     
      val (len, arrays) = descriptor.columns.zip(values).foldRight((0, List.empty[Array[Byte]])) {
        case ((QualifiedSelector(path, selector, valueType), jv), (len, acc)) =>
          val v = lengthEncoded(valueType) {
            jv match {
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
      
      arrays.foldLeft(ByteBuffer.allocate(len))((buf, arr) => buf.put(arr))
    }

    def unapply(buf: ByteBuffer) = {
      def getColumnValue(valueType: PrimitiveType) = valueType match {
        case ValueType.Long    => JInt(buf.getLong)
        case ValueType.Double  => JDouble(buf.getDouble)
        case ValueType.Boolean => JBool(buf.get != 0x0)
        case ValueType.Null    => JNull
        case ValueType.Nothing => JNothing
        case ValueType.BigDecimal(length) => //TODO: Specialize for fixed length
          val len = if (useColumnWidth) length.getOrElse(buf.getInt) else buf.remaining
          val scale = buf.getInt
          val target = new Array[Byte](len - 4)
          buf.get(target)
          JInt(new BigInt(new BigInteger(target))) //TODO: Assume that we're storing BigInt as BigDecimal

        case ValueType.String(length) => //TODO: Specialize for fixed length
          val len = if (useColumnWidth) length.getOrElse(buf.getInt) else buf.remaining
          val target = new Array[Byte](len)
          buf.get(target)
          JString(new String(target, UTF8))

      }

      descriptor.columns match {
        case valueType :: Nil => List(getColumnValue(valueType.valueType))
        case types => types.foldLeft[List[JValue]](Nil) {
          case (acc, QualifiedSelector(path, selector, valueType)) => getColumnValue(valueType) :: acc
        }
      }
    }
  }

  def projectionBijection(descriptor: ProjectionDescriptor): Bijection[JValue, ByteBuffer] = new Bijection[JValue, ByteBuffer] {
    def useColumnWidth: Boolean = true //descriptor.columns.size > 1

    def apply(jv: JValue) = {
      // Don't need to encode for single value projections
      def lengthEncoded(valueType: PrimitiveType) : Array[Byte] => Array[Byte] = {
        (a: Array[Byte]) => valueType match {
          case ValueType.BigDecimal(None)    if useColumnWidth => ByteBuffer.allocate(a.length + 4).putInt(a.length).put(a).array
          case ValueType.BigDecimal(Some(l)) if useColumnWidth => sys.error("BigDecimal may not be used fixed-length yet.")
          case ValueType.String(None)        if useColumnWidth => ByteBuffer.allocate(a.length + 4).putInt(a.length).put(a).array
          case ValueType.String(Some(l))     if useColumnWidth => ByteBuffer.allocate(l).put(a, 0, l).array
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

      arrays.foldLeft(ByteBuffer.allocate(len))((buf, arr) => buf.put(arr))
    }

    def unapply(buf: ByteBuffer) = {
      def getColumnValue(valueType: PrimitiveType) = valueType match {
        case ValueType.Long    => JInt(buf.getLong)
        case ValueType.Double  => JDouble(buf.getDouble)
        case ValueType.Boolean => JBool(buf.get != 0x0)
        case ValueType.Null    => JNull
        case ValueType.Nothing => JNothing
        case ValueType.BigDecimal(length) => //TODO: Specialize for fixed length
          val len = if (useColumnWidth) length.getOrElse(buf.getInt) else buf.remaining
          val scale = buf.getInt
          val target = new Array[Byte](len - 4)
          buf.get(target)
          JInt(new BigInt(new BigInteger(target))) //TODO: Assume that we're storing BigInt as BigDecimal

        case ValueType.String(length) => //TODO: Specialize for fixed length
          val len = if (useColumnWidth) length.getOrElse(buf.getInt) else buf.remaining
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
}

