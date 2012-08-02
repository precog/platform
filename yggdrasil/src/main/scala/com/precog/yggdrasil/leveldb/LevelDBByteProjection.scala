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
package leveldb

import serialization.bijections._
import com.precog.common._
import com.precog.util.Bijection._
import java.nio.ByteBuffer

import scalaz.Order
import scalaz.Ordering
import scalaz.syntax.bifunctor._
import scalaz.std.AllInstances._

import scala.collection.mutable.ArrayBuffer
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

    def writeValue(colDesc: ColumnDescriptor, v: CValue) = v match {
      case CString(s) => 
        val sbytes = s.getBytes("UTF-8")
        colDesc.valueType.format match {
          case FixedWidth(w) => buf.put(sbytes, 0, w)
          case LengthEncoded => buf.putInt(sbytes.length).put(sbytes)
        }
      
      case CBoolean(b) => buf.put(if (b) trueByte else falseByte)
      //case CInt(i) => buf.putInt(i)
      case CLong(l) => buf.putLong(l)
      //case CFloat(f) => buf.putFloat(f)
      case CDouble(d) => buf.putDouble(d)
      case CNum(d) => 
        val dbytes = d.as[Array[Byte]]
        buf.putInt(dbytes.length).put(dbytes)

      case null => ()
    }
    
    def toArray: Array[Byte] = buf.array
  }

  private final def listWidths(cvalues: Seq[CValue]): List[Int] = 
    descriptor.columns.map(_.valueType.format) zip cvalues map {
      case (FixedWidth(w), cv) => w
      case (LengthEncoded, CString(s)) => s.getBytes("UTF-8").length + 4
      case (LengthEncoded, CNum(n)) => n.as[Array[Byte]].length + 4
      case _ => sys.error("Column values incompatible with projection descriptor.")
    }

  object Project {
    final def singleProject(identities: Identities, cvalues: Seq[CValue]): (Array[Byte], Array[Byte]) = {
      lazy val valueWidths = listWidths(cvalues)
      
      val keyBuffer = new LevelDBWriteBuffer(8)
      keyBuffer.writeIdentity(identities(0))

      val valuesBuffer = new LevelDBWriteBuffer(valueWidths(0))
      valuesBuffer.writeValue(descriptor.columns(0), cvalues(0))
      
      (keyBuffer.toArray, valuesBuffer.toArray)
    }
  }
  
  final def toBytes(identities: Identities, cvalues: Seq[CValue]): (Array[Byte], Array[Byte]) = {
    Project.singleProject(identities, cvalues)
  }
  
  private type RBuf = ByteBuffer
  private type IdentityRead = ByteBuffer => Tuple2[Int, Long]
  private type ValueRead    = ByteBuffer => CValue
      
  private lazy val keyParsers: IndexedSeq[Either[IdentityRead, ValueRead]] = { //should be able to remove the unused variable
    (0 until descriptor.identities) map { i =>
      Left((buf: RBuf) => (i, CValueReader.readIdentity(buf))): Either[IdentityRead, ValueRead]
    } 
  }

  private lazy val valueParsers: List[ValueRead] = {
    descriptor.columns map { col => 
      (buf: RBuf) => CValueReader.readValue(buf, col.valueType)
    }
  }

  /*
  private lazy val mergeDirectives: List[Boolean] = {
    descriptor.columns.reverse.map(col => descriptor.sorting exists { 
      case (`col`, sortBy)  => sortBy == ByValue || sortBy == ByValueThenId 
      case _                => false
    }) 
  }
  */

  object Unproject {
    /*
    final def generalUnproject(keyParsers: IndexedSeq[Either[IdentityRead, ValueRead]], 
                               valueParsers: List[ValueRead], 
                               keyBytes: Array[Byte], 
                               valueBytes: Array[Byte]): (Identities,Seq[CValue]) = {
      val identitiesInKey = new ArrayBuffer[(Int,Long)](3)
      val valuesInKey = new ArrayBuffer[CValue](3)
      val valueMembers = new ArrayBuffer[CValue](3)

      val keyBuffer = ByteBuffer.wrap(keyBytes)
      val valueBuffer = ByteBuffer.wrap(valueBytes)
     
      keyParsers.foreach {
        case Left(lf)  => identitiesInKey += lf(keyBuffer)
        case Right(rf) => valuesInKey += rf(keyBuffer)
      }

      valueParsers.foreach { vf => valueMembers += vf(valueBuffer) }

      val values = mergeValues(valuesInKey, valueMembers)
      val identities = orderIdentities(identitiesInKey)

      (identities, values)
    }
    */
    
    final def singleUnproject(keyParsers: IndexedSeq[Either[IdentityRead, ValueRead]], 
                               valueParsers: List[ValueRead], 
                               keyBytes: Array[Byte], 
                               valueBytes: Array[Byte]): (Identities,Seq[CValue]) = {
      val Left(idParser) = keyParsers(0)
      val valParser = valueParsers(0)

      val keyBuffer = ByteBuffer.wrap(keyBytes)
      val valueBuffer = ByteBuffer.wrap(valueBytes)

      (Vector1(idParser(keyBuffer)._2), Vector1(valParser(valueBuffer)))
    }
    
    /*
    private final def mergeValues(keyMembers: ArrayBuffer[CValue], valueMembers: ArrayBuffer[CValue]): ArrayBuffer[CValue] = {  
      @tailrec def merge(mergeDirectives: List[Boolean], keyMembers: ArrayBuffer[CValue], valueMembers: ArrayBuffer[CValue], result: ArrayBuffer[CValue]): ArrayBuffer[CValue] = {
        mergeDirectives match {
          case true  :: ms => merge(ms, keyMembers.init, valueMembers, result += keyMembers.last)
          case false :: ms => merge(ms, keyMembers, valueMembers.init, result += valueMembers.last)
          case Nil => result.reverse
        }
      }
      merge(mergeDirectives, keyMembers, valueMembers, new ArrayBuffer(3))
    }

    private final def orderIdentities(identitiesInKey: ArrayBuffer[(Int,Long)]): VectorCase[Long] = {
      val sorted = identitiesInKey.sorted
      VectorCase.fromSeq(sorted.map(id => id._2))
    }
    */
  }

  final def fromBytes(keyBytes: Array[Byte], valueBytes: Array[Byte]): (Identities,Seq[CValue]) = {
    //if(keyParsers != null && keyParsers.size == 1 && valueParsers != null && valueParsers.size == 1) {
      Unproject.singleUnproject(keyParsers, valueParsers, keyBytes, valueBytes)
    //} else {
    //  Unproject.generalUnproject(keyParsers, valueParsers, keyBytes, valueBytes)
    //}
  }

  final def keyByteOrder: Order[Array[Byte]] = new Order[Array[Byte]] {
    def order(k1: Array[Byte], k2: Array[Byte]) = {
      val buf1 = ByteBuffer.wrap(k1)
      val buf2 = ByteBuffer.wrap(k2)

      var ordering : Ordering = Ordering.EQ
      val idOrder = Order[(Int,Long)]
      val valOrder = Order[CValue]

      var i = 0
      
      while (i < keyParsers.size && ordering == Ordering.EQ) {
        val e1 = keyParsers(i).bimap(_(buf1), _(buf1))
        val e2 = keyParsers(i).bimap(_(buf2), _(buf2))

        if (e1.isInstanceOf[Left[_,_]] && e2.isInstanceOf[Left[_,_]]) {
          ordering = idOrder.order(e1.asInstanceOf[Left[(Int,Long),CValue]].a, e2.asInstanceOf[Left[(Int,Long),CValue]].a)
        } else if (e1.isInstanceOf[Right[_,_]] && e2.isInstanceOf[Right[_,_]]) {
          ordering = valOrder.order(e1.asInstanceOf[Right[(Int,Long),CValue]].b, e2.asInstanceOf[Right[(Int,Long),CValue]].b)
        }

        i += 1
      }

      ordering
    }
  }

}

// Utility methods for CValue extraction (avoid allocation)
private[leveldb] object CValueReader {
  import LevelDBByteProjection.{trueByte, falseByte}

  final def readIdentity(buf: ByteBuffer): Long = buf.getLong

  final def readValue(buf: ByteBuffer, valueType: CType): CValue = {
    valueType match {
      case CString                => 
        val length: Int = buf.getInt
        val sstringarb: Array[Byte] = new Array(length)
        buf.get(sstringarb)
        CString(new String(sstringarb, "UTF-8"))

      case CBoolean               => 
        val b: Byte = buf.get
        if (b == trueByte)       CBoolean(true)
        else if (b == falseByte) CBoolean(false)
        else                     sys.error("Boolean byte value was not true or false")

      //case CInt                   => CInt(buf.getInt)
      case CLong                  => CLong(buf.getLong)
      //case CFloat                 => CFloat(buf.getFloat)
      case CDouble                => CDouble(buf.getDouble)
      case CNum                   => 
        val length: Int = buf.getInt
        val snum: Array[Byte] = new Array(length)
        buf.get(snum)
        CNum(snum.as[BigDecimal])

      case CEmptyArray            => null
      case CEmptyObject           => null
      case CNull                  => null

      case invalid                => sys.error("Invalid type read: " + invalid)
    }
  }
}

// vim: set ts=4 sw=4 et:
