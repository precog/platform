package com.precog.yggdrasil
package leveldb

import com.precog.common.VectorCase
import com.precog.util.Bijection._
import java.nio.ByteBuffer
import scalaz.Order
import scalaz.Ordering
import scalaz.syntax.biFunctor._
import scalaz.std.AllInstances._

import scala.collection.immutable.ListSet
import scala.annotation.tailrec

object LevelDBByteProjection {
  val trueByte: Byte = 0x01
  val falseByte: Byte = 0x00
  val nullByte: Byte = 0xff.toByte
}

trait LevelDBByteProjection extends ByteProjection {
  import LevelDBByteProjection._

  private class LevelDBWriteBuffer(length: Int) {
    private final val buf = ByteBuffer.allocate(length)
    def writeIdentity(id: Identity): Unit = buf.putLong(id)

    def writeValue(colDesc: ColumnDescriptor, v: CValue) = v.fold[Unit](
      str     = s => {
        val sbytes = s.getBytes("UTF-8")
        colDesc.valueType.format match {
          case FixedWidth(w) => buf.put(sbytes, 0, w)
          case LengthEncoded => buf.putInt(sbytes.length).put(sbytes)
        }
      },
      bool    = b => buf.put(if (b) trueByte else falseByte), 
      int     = i => buf.putInt(i), 
      long    = l => buf.putLong(l),
      float   = f => buf.putFloat(f), 
      double  = d => buf.putDouble(d), 
      num     = d => {
        val dbytes = d.as[Array[Byte]]
        buf.putInt(dbytes.length).put(dbytes)
      },
      emptyObj = (), emptyArr = (), 
      nul = colDesc.valueType match {
        case SStringFixed(width)    => buf.put(Array.fill[Byte](width)(0x00))
        case SStringArbitrary       => buf.putInt(0)
        case SBoolean               => buf.put(nullByte)
        case SInt                   => buf.putInt(Int.MaxValue)
        case SLong                  => buf.putLong(Long.MaxValue)
        case SFloat                 => buf.putFloat(Float.MaxValue)
        case SDouble                => buf.putDouble(Double.MaxValue)
        case SDecimalArbitrary      => buf.putInt(0)
        case _                      => ()
      }
    )
    
    def toArray: Array[Byte] = buf.array
  }

  private val incompatible = (_: Any) => sys.error("Column values incompatible with projection descriptor.")

  private def listWidths(cvalues: Seq[CValue]): List[Int] = 
    descriptor.columns.map(_.valueType.format) zip cvalues map {
      case (FixedWidth(w), sv) => w
      case (LengthEncoded, sv) => 
        sv.fold[Int](
          str     = s => s.getBytes("UTF-8").length + 4,
          bool    = incompatible, int     = incompatible, long    = incompatible,
          float   = incompatible, double  = incompatible, 
          num     = _.as[Array[Byte]].length + 4,
          emptyObj = incompatible(()), emptyArr = incompatible(()), nul = incompatible(())
        )
    }

  private def allocateWidth(valueWidths: Seq[Int]): (Int) =  
    descriptor.sorting.foldLeft(0) { 
      case (width, (col, ById)) =>
        (width + 8)
      case (width, (col, ByValue)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (width + valueWidths(valueIndex))

      case (width, (col, ByValueThenId)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (width + valueWidths(valueIndex) + 8)
    }


  def project(identities: Identities, cvalues: Seq[CValue]): (Array[Byte], Array[Byte]) = {
    lazy val valueWidths = listWidths(cvalues)
    val indexWidth = allocateWidth(valueWidths) 

    val (usedIdentities, usedValues): (Set[Int], Set[Int]) = descriptor.sorting.foldLeft((Set.empty[Int], Set.empty[Int])) { 
      case ((ids, values), (col, ById)) => 
        (ids + descriptor.indexedColumns(col), values)

      case ((ids, values), (col, ByValue)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (ids, values + valueIndex)

      case ((ids, values), (col, ByValueThenId)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (ids + descriptor.indexedColumns(col), values + valueIndex)
    }

    // all of the identities must be included in the key; also, any values of columns that
    // use by-value ordering must be included in the key.
    val keyBuffer = new LevelDBWriteBuffer(indexWidth + ((identities.size - usedIdentities.size) * 8))
    descriptor.sorting.foreach {
      case (col, ById)          =>
        keyBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
      case (col, ByValue)       => keyBuffer.writeValue(col, cvalues(descriptor.columns.indexOf(col)))        
      case (col, ByValueThenId) =>
        keyBuffer.writeValue(col, cvalues(descriptor.columns.indexOf(col)))
        keyBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
    }

    identities.zipWithIndex.foreach {  
      case (id, i) => if (!usedIdentities.contains(i)) keyBuffer.writeIdentity(id)
      case _ =>
    }

    val valuesBuffer = new LevelDBWriteBuffer(valueWidths.zipWithIndex collect { case (w, i) if !usedValues.contains(i) => w } sum)
    (cvalues zip descriptor.columns).zipWithIndex.foreach { 
      case ((v, col), i) if !usedValues.contains(i) => valuesBuffer.writeValue(col, v)
      case _ => 
    }

    (keyBuffer.toArray, valuesBuffer.toArray)
  }

  /** Mutable wrapper around a byte array to allow direct extraction of CValues */
  private class LevelDBReadBuffer(arr: Array[Byte]) {
    private final val buf = ByteBuffer.wrap(arr)

    def readIdentity(): Long = buf.getLong

    def readValue(valueType: ColumnType): CValue = {
      valueType match {
        case SStringFixed(width)    => 
            val sstringbuffer: Array[Byte] = new Array(width)
            buf.get(sstringbuffer)
            CString(new String(sstringbuffer, "UTF-8"))
        
        case SStringArbitrary       => 
          val length: Int = buf.getInt
          val sstringarb: Array[Byte] = new Array(length)
          buf.get(sstringarb)
          CString(new String(sstringarb, "UTF-8"))

        case SBoolean               => 
          val b: Byte = buf.get
          if (b == trueByte)       CBoolean(true)
          else if (b == falseByte) CBoolean(false)
          else                     sys.error("Boolean byte value was not true or false")

        case SInt                   => CInt(buf.getInt)
        case SLong                  => CLong(buf.getLong)
        case SFloat                 => CFloat(buf.getFloat)
        case SDouble                => CDouble(buf.getDouble)
        case SDecimalArbitrary      => 
          val length: Int = buf.getInt
          val sdecimalarb: Array[Byte] = new Array(length)
          buf.get(sdecimalarb)
          CNum(sdecimalarb.as[BigDecimal])
        case invalid                => sys.error("Invalid type read: " + invalid)
      }
    }
  }

  private type RBuf = LevelDBReadBuffer
  private type IdentityRead = LevelDBReadBuffer => Tuple2[Int, Long]
  private type ValueRead    = LevelDBReadBuffer => CValue


      
  private lazy val keyParsers: Seq[Either[IdentityRead, ValueRead]] = { //should be able to remove the unused variable
    val (initial, unused) = descriptor.sorting.foldLeft((Vector.empty[Either[IdentityRead, ValueRead]], ListSet(0.until(descriptor.identities): _*))) {
      case ((acc, ids), (col, ById))          => 
        val identityIndex = descriptor.indexedColumns(col)
        (acc :+ Left((buf:RBuf) => (identityIndex, buf.readIdentity())), ids - identityIndex)

      case ((acc, ids), (col, ByValue))       => 
        (acc :+ Right((_:RBuf).readValue(col.valueType)), ids)

      case ((acc, ids), (col, ByValueThenId)) => 
        val identityIndex = descriptor.indexedColumns(col)
        (
            acc :+ Right((_:RBuf).readValue(col.valueType)) :+ Left((buf:RBuf) => (identityIndex, buf.readIdentity())), 
            ids - identityIndex
        )
    }

    val unusedIdentities: List[IdentityRead] = unused.toList.sorted.map(i => (buf:RBuf) => (i, buf.readIdentity()))

    unusedIdentities.foldLeft(initial) {
      case (acc, id) => acc :+ Left(id) 
    }
  }


  private lazy val valueParsers: List[ValueRead] = {
    descriptor.columns filter { col => 
      !(descriptor.sorting exists { 
        case (`col`, sortBy)  => sortBy == ByValue || sortBy == ByValueThenId
        case _                => false
      })
    } map { col => 
      (_: RBuf).readValue(col.valueType)
    }
  }

  private lazy val mergeDirectives: List[Boolean] = {
    descriptor.columns.map(col => descriptor.sorting exists { 
      case (`col`, sortBy)  => sortBy == ByValue || sortBy == ByValueThenId 
      case _                => false
    }) 
  }

  private def mergeValues(keyMembers: Vector[CValue], valueMembers: Vector[CValue]): List[CValue] = {  
    @tailrec def merge(mergeDirectives: List[Boolean], keyMembers: Vector[CValue], valueMembers: Vector[CValue], result: List[CValue]): List[CValue] = {
      mergeDirectives match {
        case true  :: ms => merge(ms, keyMembers.init, valueMembers, keyMembers.last :: result)
        case false :: ms => merge(ms, keyMembers, valueMembers.init, valueMembers.last :: result)
        case Nil => result
      }
    }
    merge(mergeDirectives.reverse, keyMembers, valueMembers, Nil)
  }

  private def orderIdentities(identitiesInKey: Vector[(Int,Long)]): VectorCase[Long] = {
    val sorted = identitiesInKey.sorted
    VectorCase.fromSeq(sorted.map(id => id._2))
  }

  def unproject[E](keyBytes: Array[Byte], valueBytes: Array[Byte])(f: (Identities, Seq[CValue]) => E): E = {
    val (_, (identitiesInKey, valuesInKey)) = keyParsers.foldLeft((new LevelDBReadBuffer(keyBytes), (Vector.empty[(Int,Long)], Vector.empty[CValue]))) {
      case ((buf, (ids, values)), Left(f)) => (buf, ((ids :+ f(buf)), values))
      case ((buf, (ids, values)), Right(f)) => (buf, (ids, values :+ f(buf)))
    }

    val (_, valueMembers) = valueParsers.foldLeft((new LevelDBReadBuffer(valueBytes), Vector.empty[CValue])) {
      case ((buf, acc), f) => (buf, acc :+ f(buf))
    }

    val values = mergeValues(valuesInKey, valueMembers)
    val identities = orderIdentities(identitiesInKey)

    f(identities, values)
  }

  def keyOrder: Order[Array[Byte]] = new Order[Array[Byte]] {
    def order(k1: Array[Byte], k2: Array[Byte]) = {
      val (_, elements1) = keyParsers.foldRight((new LevelDBReadBuffer(k1), List.empty[Either[(Int,Long), CValue]])) {
        case (f, (buf, acc)) => (buf, f.bimap(_(buf), _(buf)) :: acc)
      }

      val (_, elements2) = keyParsers.foldRight((new LevelDBReadBuffer(k2), List.empty[Either[(Int,Long), CValue]])) {
        case (f, (buf, acc)) => (buf, f.bimap(_(buf), _(buf)) :: acc)
      }

      (elements1 zip elements2).foldLeft[Ordering](Ordering.EQ) {
        case (Ordering.EQ, (Left(e1),  Left(e2)))  => Order[(Int,Long)].order(e1, e2)
        case (Ordering.EQ, (Right(e1), Right(e2))) => Order[CValue].order(e1, e2)
        case (other, _) => other
      }
    }
  }

}

// vim: set ts=4 sw=4 et:
