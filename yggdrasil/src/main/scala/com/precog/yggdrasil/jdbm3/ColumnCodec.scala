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
package jdbm3

import com.precog.yggdrasil.table._

import blueeyes.json.JPath
import org.joda.time.DateTime
import com.weiglewilczek.slf4s.Logging

import java.nio.{ ByteBuffer, CharBuffer }
import java.nio.charset.{ Charset, CharsetEncoder, CoderResult }

import scala.collection.mutable.ArrayBuffer

import scala.annotation.tailrec
import scala.{ specialized => spec }

import java.math.MathContext


// object ColumnCodec {
//   val readOnly = new ColumnCodec(0) // For read only work, the buffer is always provided
// }
// 
// /**
//  * This class is responsible for encoding and decoding a Seq[(ColumnRef,Column)]
//  * into a byte array for serialization. It is *not* thread-safe.
//  */
// class ColumnCodec(bufferSize: Int = (16 * 1024)) extends Logging {
//   import CTypeMappings._
// 
//   private final val workBuffer = ByteBuffer.allocate(bufferSize)
//   private final val DefaultCharset = "UTF-8"
//   
//   private final val FALSE_VALUE = 0.toByte
//   private final val TRUE_VALUE = 1.toByte
// 
//   def write[A](cType: CValueType[A], a: A): Unit = cType match {
//     case CString => writeString(a)
//     case CBoolean => writeBoolean(a)
//     case CLong => workBuffer.putLong(a)
//     case CDouble => workBuffer.putDouble(a)
//     case CNum => writeBigDecimal(a)
//     case CDate => workBuffer.putLong(a.getMillis)
//     case CArrayType(elemType) =>
//       workBuffer.putInt(a.length)
//       a foreach (write(elemType, _))
//   }
// 
//   // This generates a NoSuchMethod error for array types, for some reason.
//   //private def writerFor[A](cType: CValueType[A]): (A => Unit) = cType match {
//     //case CString => writeString(_)
//     //case CBoolean => writeBoolean(_)
//     //case CLong => workBuffer.putLong(_)
//     //case CDouble => workBuffer.putDouble(_)
//     //case CNum => writeBigDecimal(_)
//     //case CDate => dt => workBuffer.putLong(dt.getMillis)
//     //case CArrayType(elemType) => writeArray(_, writerFor(elemType))
//   //}
// 
//   private def writeArray[A](s: IndexedSeq[A], w: A => Unit) {
//     workBuffer.putInt(s.length)
//     s foreach { a =>
//       w(a)
//     }
//   }
// 
//   private def writeString(s: String) {
//     // RLE Strings
//     val bytes = s.getBytes(DefaultCharset)
//     workBuffer.putInt(bytes.length)
//     workBuffer.put(bytes)
//   }
// 
//   private def writeBigDecimal(bd: BigDecimal) {
//     writeString(bd.toString) // TODO: Figure out a sane way to serialize BigDecimal
//   }
// 
//   private def writeBoolean(b: Boolean) {
//     workBuffer.put(if (b) TRUE_VALUE else FALSE_VALUE)
//   }
// 
//   private def readerFor[A](cType: CValueType[A]): ByteBuffer => A = cType match {
//     case CString => readString(_)
//     case CBoolean => readBoolean(_)
//     case CLong => _.getLong()
//     case CDouble => _.getDouble()
//     case CNum => readBigDecimal(_)
//     case CDate => buffer => new DateTime(buffer.getLong())
//     case CArrayType(elemType) =>
//       readArray(_, readerFor(elemType))
//   }
// 
//   private def readArray[A](buffer: ByteBuffer, readElem: ByteBuffer => A) = {
//     val length = buffer.getInt()
//     ((0 until length) map (_ => readElem(buffer))).toIndexedSeq
//   }
// 
//   private def readString(buffer: ByteBuffer): String = {
//     // TODO: Could possibly be more efficient here with allocations
//     val bytes = new Array[Byte](buffer.getInt())
//     buffer.get(bytes)
//     new String(bytes, DefaultCharset)
//   }
// 
//   private def readBigDecimal(buffer: ByteBuffer): BigDecimal = {
//     BigDecimal(readString(buffer), java.math.MathContext.UNLIMITED)
//   }
// 
//   private def readBoolean(buffer: ByteBuffer): Boolean = {
//     buffer.get() match {
//       case TRUE_VALUE  => true
//       case FALSE_VALUE => false
//       case invalid     => sys.error("Invalid boolean encoded value: " + invalid)
//     }
//   }
// 
//   @tailrec
//   private def writeFlagFor(cType: CType) {
//     workBuffer.put(flagFor(cType))
//     cType match {
//       case CArrayType(elemType) =>
//         writeFlagFor(elemType)
//       case _ =>
//     }
//   }
// 
//   def encode(values: Seq[CValue]): Array[Byte] = {
//     workBuffer.clear()
// 
//     values.foreach { value => {
//       if (value == CUndefined) {
//         workBuffer.put(FUNDEFINED)
//       } else {
//         writeFlagFor(value.cType)
//         value match {
//           case CString(cs)  => writeString(cs)                  
//           case CBoolean(cb) => writeBoolean(cb)
//           case CLong(cl)    => workBuffer.putLong(cl)
//           case CDouble(cd)  => workBuffer.putDouble(cd)
//           case CNum(cn)     => writeBigDecimal(cn)
//           case CDate(cd)    => workBuffer.putLong(cd.getMillis)
//           case CArray(as, cType) => write(cType, as)
//           case CNull        => // NOOP, no value to write
//           case CEmptyObject => // NOOP, no value to write
//           case CEmptyArray  => // NOOP, no value to write
//           case CUndefined   => // NOOP, no value to write
//         }
//       }
//     }}
// 
//     // Read bytes out of the bytebuffer into a new array
//     val outBytes = new Array[Byte](workBuffer.position())
//     workBuffer.flip()
//     workBuffer.get(outBytes)
// 
//     outBytes
//   }
// 
//   def encode(columns: Seq[(ColumnRef, Column)], row: Int, encodeRef: Boolean = false): Array[Byte] = {
//     workBuffer.clear()
// 
//     columns.foreach {
//       case (ref, column) => {
//         if (encodeRef) {
//           writeString(ref.selector.toString)
//         }
//         
//         if (column.isDefinedAt(row)) {
//           workBuffer.put(flagFor(ref.ctype))
//           ref.ctype match {
//             case CString      => writeString(column.asInstanceOf[StrColumn].apply(row))                  
//             case CBoolean     => writeBoolean(column.asInstanceOf[BoolColumn].apply(row))                
//             case CLong        => workBuffer.putLong(column.asInstanceOf[LongColumn].apply(row))          
//             case CDouble      => workBuffer.putDouble(column.asInstanceOf[DoubleColumn].apply(row))      
//             case CNum         => writeBigDecimal(column.asInstanceOf[NumColumn].apply(row))              
//             case CDate        => workBuffer.putLong(column.asInstanceOf[DateColumn].apply(row).getMillis)
//             case CArrayType(elemType) => sys.error("TODO: cannot encode array columns yet")
//             case CNull        => // No value encoded
//             case CEmptyObject => // No value encoded
//             case CEmptyArray  => // No value encoded
//             case CUndefined   => sys.error("Cannot encode an undefined column")
//           }
//         } else {
//           workBuffer.put(FUNDEFINED)
//         }
//       }
//     }
// 
//     // Read bytes out of the bytebuffer into a new array
//     val outBytes = new Array[Byte](workBuffer.position())
//     workBuffer.flip()
//     workBuffer.get(outBytes)
// 
//     outBytes
//   }
// 
//   private[jdbm3] def readCType(buffer: ByteBuffer): CType = buffer.get() match {
//     case FSTRING => CString
//     case FBOOLEAN => CBoolean
//     case FLONG => CLong
//     case FDOUBLE => CDouble
//     case FNUM => CNum
//     case FDATE => CDate
//     case FARRAY =>
//       readCType(buffer) match {
//         case elemType: CValueType[_] => CArrayType(elemType)
//         case badType => sys.error("Invalid array element type: " + badType)
//       }
//     case FNULL => CNull
//     case FEMPTYOBJECT => CEmptyObject
//     case FEMPTYARRAY => CEmptyArray
//     case FUNDEFINED => CUndefined
//     case invalid => sys.error("Invalid format flag: " + invalid)
//   }
// 
//   private def readToCValue(buffer: ByteBuffer): CValue = {
//     readCType(buffer) match {
//       case cType: CValueType[_] => cType(readerFor(cType)(buffer))
//       case cType: CNullType => cType
//     }
//   }
// 
//   def decodeWithRefs(input: Array[Byte]): Array[(String, CValue)] = decodeWithRefs(ByteBuffer.wrap(input))
// 
//   def decodeWithRefs(buffer: ByteBuffer): Array[(String, CValue)] = {
//     var resultBuffer = ArrayBuffer[(String, CValue)]()
// 
//     while (buffer.hasRemaining()) {
//       val selector = readString(buffer)
//       resultBuffer.append((selector, readToCValue(buffer)))
//     }
// 
//     resultBuffer.toArray
//   }
// 
//   def decodeToCValues(input: Array[Byte]): Array[CValue] = decodeToCValues(ByteBuffer.wrap(input))
// 
//   def decodeToCValues(buffer: ByteBuffer): Array[CValue] = {
//     var resultBuffer = ArrayBuffer[CValue]()
// 
//     while (buffer.hasRemaining()) {
//       resultBuffer.append(readToCValue(buffer))
//     }
// 
//     resultBuffer.toArray
//   }
// 
//   def decodeToArrayColumns(input: Array[Byte], row: Int, columns: Array[ArrayColumn[_]]) {
//     decodeToArrayColumns(ByteBuffer.wrap(input), row, columns)
//   }
// 
//   /**
//    * Decode the given byte buffer, storing its values in the proper ArrayColumns
//    * @param buffer The buffer to read from. Must be ready for reads (position == 0)
//    */
//   def decodeToArrayColumns(buffer: ByteBuffer, row: Int, columns: Array[ArrayColumn[_]]) {
//     var columnIndex = 0
// 
//     while (buffer.hasRemaining()) {
//       readCType(buffer) match {
//         case CString       => columns(columnIndex).asInstanceOf[ArrayStrColumn].update(row, readString(buffer))
//         case CBoolean      => columns(columnIndex).asInstanceOf[ArrayBoolColumn].update(row, readBoolean(buffer))
//         case CLong         => columns(columnIndex).asInstanceOf[ArrayLongColumn].update(row, buffer.getLong())
//         case CDouble       => columns(columnIndex).asInstanceOf[ArrayDoubleColumn].update(row, buffer.getDouble())
//         case CNum          => columns(columnIndex).asInstanceOf[ArrayNumColumn].update(row, readBigDecimal(buffer))
//         case CDate         => columns(columnIndex).asInstanceOf[ArrayDateColumn].update(row, new DateTime(buffer.getLong()))
//         case cType @ CArrayType(_) =>
//           val col = columns(columnIndex).asInstanceOf[ArrayHomogeneousArrayColumn[AnyRef]]
//           col.update(row, readerFor(cType)(buffer).asInstanceOf[IndexedSeq[AnyRef]])
//         case CNull         => columns(columnIndex).asInstanceOf[MutableNullColumn].update(row, true)
//         case CEmptyObject  => columns(columnIndex).asInstanceOf[MutableEmptyObjectColumn].update(row, true)
//         case CEmptyArray   => columns(columnIndex).asInstanceOf[MutableEmptyArrayColumn].update(row, true)          
//         case CUndefined    => // NOOP, array/mutable columns start fully undefined                                  
//         case invalid       => sys.error("Invalid format flag: " + invalid)                                          
//       }                                                                                                             
//       columnIndex += 1                                                                                              
//     }                                                                                                               
//   }                                                                                                                 
// }                                                                                                                   
                                                                                                                    
                                                                                                                    
