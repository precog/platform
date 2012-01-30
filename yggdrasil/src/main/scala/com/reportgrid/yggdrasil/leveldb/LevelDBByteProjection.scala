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
package com.reportgrid.yggdrasil
package leveldb

import com.reportgrid.util.Bijection._
import java.nio.ByteBuffer

trait BufferConstants {
    val trueByte: Byte = 0x01
    val falseByte: Byte = 0x00
    val nullByte: Byte = 0xff.toByte
}

private[leveldb] class LevelDBWriteBuffer(length: Int) extends BufferConstants {
  
  //println("length = " + length)

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

private[leveldb] class LevelDBReadBuffer(arr: Array[Byte]) extends BufferConstants {

  private final val buf = ByteBuffer.wrap(arr)

  def readIdentity(): Long = buf.getLong

  def readValue(valueType: ColumnType): CValue = {
    valueType match {
      case SStringFixed(width)    => {
          val sstringbuffer: Array[Byte] = new Array(width)
          buf.get(sstringbuffer)
          CString(new String(sstringbuffer, "UTF-8"))
      }
      case SStringArbitrary       => {
          val length: Int = buf.getInt
          val sstringarb: Array[Byte] = new Array(length)
          buf.get(sstringarb)
          CString(new String(sstringarb, "UTF-8"))
      }
      case SBoolean               => buf.get match {
          case b if b == trueByte => CBoolean(true)
          case b if b == falseByte => CBoolean(false)
          case _ => sys.error("Boolean byte value was not true or false")
      }
      case SInt                   => CInt(buf.getInt)
      case SLong                  => CLong(buf.getLong)
      case SFloat                 => CFloat(buf.getFloat)
      case SDouble                => CDouble(buf.getDouble)
      case SDecimalArbitrary      => {
          val length: Int = buf.getInt
          val sdecimalarb: Array[Byte] = new Array(length)
          buf.get(sdecimalarb)
          CNum(sdecimalarb.as[BigDecimal])


      }
      case _                      => sys.error("unhandled ColumnType; need to implement")
    }
  }
    
    
}

trait LevelDBByteProjection extends ByteProjection {
  private val incompatible = (_: Any) => sys.error("Column values incompatible with projection descriptor.")

  def listWidths(cvalues: Seq[CValue]): List[Int] = descriptor.columns.map(_.valueType.format) zip cvalues map {
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

  def allocateWidth(valueWidths: Seq[Int]): (Int) = 
    descriptor.sorting.foldLeft(0) { 
      case (width, (col, ById)) =>
        if (descriptor.indexedColumns.map(_._2).toList.indexOf(descriptor.indexedColumns(col)) == descriptor.columns.indexOf(col)) (width + 8)
        else width

      case (width, (col, ByValue)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        (width + valueWidths(valueIndex))

      case (width, (col, ByValueThenId)) => 
        val valueIndex = descriptor.columns.indexOf(col)
        if (descriptor.indexedColumns.map(_._2).toList.indexOf(descriptor.indexedColumns(col)) == descriptor.columns.indexOf(col)) (width + valueWidths(valueIndex) + 8)
        else (width + valueWidths(valueIndex))

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
    val indexBuffer = new LevelDBWriteBuffer(indexWidth + ((identities.size - usedIdentities.size) * 8))
    descriptor.sorting.foreach {
      case (col, ById)          =>
        //checks that the identity has not been written already
        if (descriptor.indexedColumns.map(_._2).toList.indexOf(descriptor.indexedColumns(col)) == descriptor.columns.indexOf(col))
          indexBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
      case (col, ByValue)       => indexBuffer.writeValue(col, cvalues(descriptor.columns.indexOf(col)))        
      case (col, ByValueThenId) =>
        indexBuffer.writeValue(col, cvalues(descriptor.columns.indexOf(col)))
        if (descriptor.indexedColumns.map(_._2).toList.indexOf(descriptor.indexedColumns(col)) == descriptor.columns.indexOf(col))
          indexBuffer.writeIdentity(identities(descriptor.indexedColumns(col)))
    }

    identities.zipWithIndex.foreach {
      case (id, i) => if (!usedIdentities.contains(i)) indexBuffer.writeIdentity(id)
      case _ =>
    }

    val valuesBuffer = new LevelDBWriteBuffer(valueWidths.zipWithIndex collect { case (w, i) if !usedValues.contains(i) => w } sum)
    (cvalues zip descriptor.columns).zipWithIndex.foreach { //by zip we lose the extra cvalues not in bijection with descriptor - is this correct?
      case ((v, col), i) if !usedValues.contains(i) => valuesBuffer.writeValue(col, v)
      case _ => 
    }

    (indexBuffer.toArray, valuesBuffer.toArray)
  }

  type ElementFormat = LevelDBReadBuffer => CValue
  def eFormat(t: ColumnType): ElementFormat = _.readValue(t)


  def unproject[E](keyBytes: Array[Byte], valueBytes: Array[Byte])(f: (Identities, Seq[CValue]) => E): E = {

    
    
    def indexFormat: List[ElementFormat] = {

      val (initial, unused) = descriptor.sorting.foldLeft((List[ElementFormat](), 0.until(descriptor.identities).toList)) {
        case ((acc, ids), (col, ById))          => (acc :+ eFormat(SLong), ids.filter( _ != descriptor.indexedColumns(col)))
        case ((acc, ids), (col, ByValue))       => (acc :+ eFormat(col.valueType), ids)
        case ((acc, ids), (col, ByValueThenId)) => (acc ++ (eFormat(col.valueType) :: eFormat(SLong) :: Nil), ids.filter( _ != descriptor.indexedColumns(col)))
      }

      unused.foldLeft(initial) {
        case (acc, idx) => acc :+ eFormat(SLong)
      }
    }

    def valueFormat: List[ElementFormat] = 
      descriptor.sorting.foldLeft(descriptor.columns) {
        case (acc, (_, ById)) => acc
        case (acc, (col, _))  => acc.filter( _ != col)
      } map { col => eFormat(col.valueType) }

    def extractIdentities(indexMembers: Seq[CValue]): Identities = {
      val (initial, unused) = descriptor.sorting.foldLeft((List[Int](), 0.until(descriptor.identities).toList)) {
        case ((acc, ids), (col, ById))          => (acc :+ descriptor.indexedColumns(col), ids.filter( _ != descriptor.indexedColumns(col)))
        case ((acc, ids), (_, ByValue))         => (acc :+ -1, ids)
        case ((acc, ids), (col, ByValueThenId)) => (acc ++ (descriptor.indexedColumns(col) :: -1 :: Nil), ids.filter( _ != descriptor.indexedColumns(col)))
      }

      val mapping = unused.foldLeft(initial) {
        case (acc, el) => acc :+ el
      }.zipWithIndex.filter( _._1 == -1 ).sortBy( _._1 ).map( _._2 )

      mapping.foldLeft( Vector[Long]() ) {
        case (acc, source) => indexMembers(source) match {
          case CLong(l) => acc :+ l
          case _        => sys.error("Invalid index type mapping")
        }
      }
    }

    def extractValues(indexMembers: Seq[CValue], valueMembers: Seq[CValue]): Seq[CValue] = {
      
      val (_, values) = descriptor.sorting.foldLeft((indexMembers, List[(Int, CValue)]())) {
        case ((id :: tail, acc), (_, ById))                     => (tail, acc)
        case ((value :: tail, acc), (col, ByValue))             => (tail, acc :+ (descriptor.columns.indexOf(col) -> value))
        case ((value :: id :: tail, acc), (col, ByValueThenId)) => (tail, acc :+ (descriptor.columns.indexOf(col) -> value))
      }

      descriptor.sorting.foldLeft(descriptor.columns) {
        case (acc, (_, ById)) => acc
        case (acc, (col, _))  => acc.filter( _ != col)
      }.zipWithIndex.foldLeft(values) {
        case (acc, (col, index)) => acc :+ (index -> valueMembers(index)) 
      }.sortBy(_._1).map(_._2)
    
    }

    val indexBuffer = new LevelDBReadBuffer(keyBytes)
    val valueBuffer = new LevelDBReadBuffer(valueBytes)

    val indexMembers = indexFormat.map( _(indexBuffer) )
    val valueMembers = valueFormat.map( _(valueBuffer) )

    val identities = extractIdentities(indexMembers)
    val values = extractValues(indexMembers, valueMembers)
    
    f(identities, values)
  }
}

// vim: set ts=4 sw=4 et:
